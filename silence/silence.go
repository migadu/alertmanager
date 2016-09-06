// Copyright 2016 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package silence provides a storage for silences, which can share its
// state over a mesh network and snapshot it.
package silence

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	pb "github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/satori/go.uuid"
	"github.com/weaveworks/mesh"
)

// ErrNotFound is returned if a silence was not found.
var ErrNotFound = fmt.Errorf("not found")

func utcNow() time.Time {
	return time.Now().UTC()
}

// Silences holds a silence state that can be modified, queried, and snapshot.
type Silences struct {
	logger    log.Logger
	now       func() time.Time
	retention time.Duration

	gossip mesh.Gossip // gossip channel for sharing silences

	// We store silences in a map of IDs for now. Currently, the memory
	// state is equivalent to the mesh.GossipData representation.
	// In the future we'll want support for efficient queries by time
	// range and affected labels.
	mtx sync.RWMutex
	st  gossipData
}

// Options exposes configuration options for creating a new Silences object.
// Its zero value is a safe default.
type Options struct {
	// A snapshot file or reader from which the initial state is loaded.
	// None or only one of them must be set.
	SnapshotFile   string
	SnapshotReader io.Reader

	// Retention time for newly created Silences. Silences may be
	// garbage collected after the given duration after they ended.
	Retention time.Duration

	// A function creating a mesh.Gossip on being called with a mesh.Gossiper.
	Gossip func(g mesh.Gossiper) mesh.Gossip

	// A logger used by background processing.
	Logger log.Logger
}

func (o *Options) validate() error {
	if o.SnapshotFile != "" && o.SnapshotReader != nil {
		return fmt.Errorf("only one of SnapshotFile and SnapshotReader must be set")
	}
	return nil
}

// New returns a new Silences object with the given configuration.
func New(o Options) (*Silences, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if o.SnapshotFile != "" {
		if r, err := os.Open(o.SnapshotFile); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else {
			o.SnapshotReader = r
		}
	}
	s := &Silences{
		logger:    log.NewNopLogger(),
		retention: o.Retention,
		now:       utcNow,
		gossip:    nopGossip{},
		st:        gossipData{},
	}
	if o.Logger != nil {
		s.logger = o.Logger
	}
	if o.Gossip != nil {
		s.gossip = o.Gossip(gossiper{s})
	}
	if o.SnapshotReader != nil {
		if err := s.loadSnapshot(o.SnapshotReader); err != nil {
			return s, err
		}
	}
	return s, nil
}

type nopGossip struct{}

func (nopGossip) GossipBroadcast(d mesh.GossipData)         {}
func (nopGossip) GossipUnicast(mesh.PeerName, []byte) error { return nil }

// Maintenance garbage collects the silence state at the given interval. If the snapshot
// file is set, a snapshot is written to it afterwards.
// Terminates on receiving from stopc.
func (s *Silences) Maintenance(interval time.Duration, snapf string, stopc <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()

	f := func() error {
		start := s.now()
		s.logger.Info("running maintenance")
		defer s.logger.With("duration", s.now().Sub(start)).Info("maintenance done")

		if _, err := s.GC(); err != nil {
			return err
		}
		if snapf == "" {
			return nil
		}
		f, err := openReplace(snapf)
		if err != nil {
			return err
		}
		// TODO(fabxc): potentially expose snapshot size in log message.
		if _, err := s.Snapshot(f); err != nil {
			return err
		}
		return f.Close()
	}

Loop:
	for {
		select {
		case <-stopc:
			break Loop
		case <-t.C:
			if err := f(); err != nil {
				s.logger.With("err", err).Error("running maintenance failed")
			}
		}
	}
	// No need for final maintenance if we don't want to snapshot.
	if snapf == "" {
		return
	}
	if err := f(); err != nil {
		s.logger.With("err", err).Info("msg", "creating shutdown snapshot failed")
	}
}

// GC runs a garbage collection that removes silences that have ended longer
// than the configured retention time ago.
func (s *Silences) GC() (int, error) {
	now, err := s.nowProto()
	if err != nil {
		return 0, err
	}
	var n int

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for id, sil := range s.st {
		if !protoBefore(now, sil.ExpiresAt) {
			delete(s.st, id)
			n++
		}
	}
	return n, nil
}

func protoBefore(a, b *timestamp.Timestamp) bool {
	if a.Seconds > b.Seconds {
		return false
	}
	if a.Seconds == b.Seconds {
		return a.Nanos < b.Nanos
	}
	return true
}

func validateMatcher(m *pb.Matcher) error {
	if !model.LabelName(m.Name).IsValid() {
		return fmt.Errorf("invalid label name %q", m.Name)
	}
	switch m.Type {
	case pb.Matcher_EQUAL:
		if !model.LabelValue(m.Pattern).IsValid() {
			return fmt.Errorf("invalid label value %q", m.Pattern)
		}
	case pb.Matcher_REGEXP:
		if _, err := regexp.Compile(m.Pattern); err != nil {
			return fmt.Errorf("invalid regular expression %q: %s", m.Pattern, err)
		}
	default:
		return fmt.Errorf("unknown matcher type %q", m.Type)
	}
	return nil
}

func validateSilence(s *pb.Silence) error {
	if s.Id == "" {
		return errors.New("ID missing")
	}
	if len(s.Matchers) == 0 {
		return errors.New("at least one matcher required")
	}
	for i, m := range s.Matchers {
		if err := validateMatcher(m); err != nil {
			return fmt.Errorf("invalid label matcher %d: %s", i, err)
		}
	}
	startsAt, err := ptypes.Timestamp(s.StartsAt)
	if err != nil {
		return fmt.Errorf("invalid start time: %s", err)
	}
	endsAt, err := ptypes.Timestamp(s.EndsAt)
	if err != nil {
		return fmt.Errorf("invalid end time: %s", err)
	}
	if endsAt.Before(startsAt) {
		return errors.New("end time must not be before start time")
	}
	if _, err := ptypes.Timestamp(s.UpdatedAt); err != nil {
		return fmt.Errorf("invalid update timestamp: %s", err)
	}
	return nil
}

// cloneSilence returns a shallow copy of a silence.
func cloneSilence(sil *pb.Silence) *pb.Silence {
	s := *sil
	return &s
}

func (s *Silences) getSilence(id string) (*pb.Silence, bool) {
	msil, ok := s.st[id]
	if !ok {
		return nil, false
	}
	return msil.Silence, true
}

func (s *Silences) setSilence(sil *pb.Silence) error {
	endsAt, err := ptypes.Timestamp(sil.EndsAt)
	if err != nil {
		return err
	}
	expiresAt, err := ptypes.TimestampProto(endsAt.Add(s.retention))
	if err != nil {
		return err
	}
	msil := &pb.MeshSilence{
		Silence:   sil,
		ExpiresAt: expiresAt,
	}
	st := gossipData{sil.Id: msil}

	s.st.Merge(st)
	s.gossip.GossipBroadcast(st)

	return nil
}

func (s *Silences) nowProto() (*timestamp.Timestamp, error) {
	now := s.now()
	return ptypes.TimestampProto(now)
}

// Create adds a new silence and returns its ID.
func (s *Silences) Create(sil *pb.Silence) (id string, err error) {
	if sil.Id != "" {
		return "", fmt.Errorf("unexpected ID in new silence")
	}
	sil.Id = uuid.NewV4().String()

	now, err := s.nowProto()
	if err != nil {
		return "", err
	}
	if sil.StartsAt == nil {
		sil.StartsAt = now
	} else if protoBefore(sil.StartsAt, now) {
		return "", fmt.Errorf("new silence must not start in the past")
	}
	sil.UpdatedAt = now

	if err := validateSilence(sil); err != nil {
		return "", fmt.Errorf("invalid silence: %s", err)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if err := s.setSilence(sil); err != nil {
		return "", err
	}
	return sil.Id, nil
}

// Expire the silence with the given ID immediately.
func (s *Silences) Expire(id string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	sil, ok := s.getSilence(id)
	if !ok {
		return ErrNotFound
	}

	now, err := s.nowProto()
	if err != nil {
		return err
	}
	if sil, err = silenceSetTimeRange(sil, now, sil.StartsAt, now); err != nil {
		return err
	}
	return s.setSilence(sil)
}

// SetTimeRange adjust the time range of a silence if allowed. If start or end
// are zero times, the current value remains unmodified.
func (s *Silences) SetTimeRange(id string, start, end time.Time) error {
	now, err := s.nowProto()
	if err != nil {
		return err
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	sil, ok := s.getSilence(id)
	if !ok {
		return ErrNotFound
	}

	// Retrieve protobuf start and end time, default to current value
	// of the silence.
	var startp, endp *timestamp.Timestamp
	if start.IsZero() {
		startp = sil.StartsAt
	} else if startp, err = ptypes.TimestampProto(start); err != nil {
		return err
	}
	if end.IsZero() {
		endp = sil.EndsAt
	} else if endp, err = ptypes.TimestampProto(end); err != nil {
		return err
	}

	if sil, err = silenceSetTimeRange(sil, now, startp, endp); err != nil {
		return err
	}
	return s.setSilence(sil)
}

func silenceSetTimeRange(sil *pb.Silence, now, start, end *timestamp.Timestamp) (*pb.Silence, error) {
	if protoBefore(end, start) {
		return nil, fmt.Errorf("end time must not be before start time")
	}
	// Validate modification based on current silence state.
	switch getState(sil, now) {
	case StateActive:
		if *start != *sil.StartsAt {
			return nil, fmt.Errorf("start time of active silence cannot be modified")
		}
		if protoBefore(end, now) {
			return nil, fmt.Errorf("end time cannot be set into the past")
		}
	case StatePending:
		if protoBefore(start, now) {
			return nil, fmt.Errorf("start time cannot be set into the past")
		}
	case StateExpired:
		return nil, fmt.Errorf("expired silence must not be modified")
	default:
		panic("unknown silence state")
	}

	sil = cloneSilence(sil)
	sil.StartsAt = start
	sil.EndsAt = end
	sil.UpdatedAt = now

	return sil, nil
}

// AddComment adds a new comment to the silence with the given ID.
func (s *Silences) AddComment(id string, author, comment string) error {
	panic("not implemented")
}

// QueryParam expresses parameters along which silences are queried.
type QueryParam func(*query) error

type query struct {
	ids     []string
	filters []silenceFilter
}

// silenceFilter is a function that returns true if a silence
// should be dropped from a result set for a given time.
type silenceFilter func(*pb.Silence, *timestamp.Timestamp) (bool, error)

var errNotSupported = errors.New("query parameter not supported")

// QIDs configures a query to select the given silence IDs.
func QIDs(ids ...string) QueryParam {
	return func(q *query) error {
		q.ids = append(q.ids, ids...)
		return nil
	}
}

// QTimeRange configures a query to search for silences that are active
// in the given time range.
// TODO(fabxc): not supported yet.
func QTimeRange(start, end time.Time) QueryParam {
	return func(q *query) error {
		return errNotSupported
	}
}

// QMatches returns silences that match the given label set.
func QMatches(set map[string]string) QueryParam {
	return func(q *query) error {
		f := func(s *pb.Silence, _ *timestamp.Timestamp) (bool, error) {
			// TODO(fabxc): we compile every regexp matcher of a silence
			// each time we check it against a label set.
			// This could be notably slower than the old caching behavior.
			// With the protobuf type not being extensible, we need a more
			// efficient solution without wrapping the silence in another layer.
			for _, m := range s.Matchers {
				switch m.Type {
				case pb.Matcher_EQUAL:
					if set[m.Name] != m.Pattern {
						return false, nil
					}
				case pb.Matcher_REGEXP:
					re, err := regexp.Compile(m.Pattern)
					if err != nil {
						return false, err
					}
					if !re.MatchString(set[m.Name]) {
						return false, nil
					}
				}
			}
			// All matchers applied to the given set and the silence
			// passes as a result.
			return true, nil
		}
		q.filters = append(q.filters, f)
		return nil
	}
}

// SilenceState describes the state of a silence based on its time range.
type SilenceState string

// The only possible states of a silence w.r.t a timestamp.
const (
	StateActive  SilenceState = "active"
	StatePending              = "pending"
	StateExpired              = "expired"
)

// getState returns a silence's SilenceState at the given timestamp.
func getState(sil *pb.Silence, ts *timestamp.Timestamp) SilenceState {
	if protoBefore(ts, sil.StartsAt) {
		return StatePending
	}
	if protoBefore(sil.EndsAt, ts) {
		return StateExpired
	}
	return StateActive
}

// QState filters queried silences by the given states.
func QState(states ...SilenceState) QueryParam {
	return func(q *query) error {
		f := func(sil *pb.Silence, now *timestamp.Timestamp) (bool, error) {
			s := getState(sil, now)

			for _, ps := range states {
				if s == ps {
					return true, nil
				}
			}
			return false, nil
		}
		q.filters = append(q.filters, f)
		return nil
	}
}

// Query for silences based on the given query parameters.
func (s *Silences) Query(params ...QueryParam) ([]*pb.Silence, error) {
	q := &query{}
	for _, p := range params {
		if err := p(q); err != nil {
			return nil, err
		}
	}
	nowpb, err := s.nowProto()
	if err != nil {
		return nil, err
	}
	return s.query(q, nowpb)
}

func (s *Silences) query(q *query, now *timestamp.Timestamp) ([]*pb.Silence, error) {
	// If we have an ID constraint, all silences are our base set.
	// This and the use of post-filter functions is the
	// the trivial solution for now.
	var res []*pb.Silence

	s.mtx.RLock()
	if q.ids != nil {
		for _, id := range q.ids {
			if s, ok := s.st[string(id)]; ok {
				res = append(res, s.Silence)
			}
		}
	} else {
		for _, sil := range s.st {
			res = append(res, sil.Silence)
		}
	}
	s.mtx.RUnlock()

	var resf []*pb.Silence
	for _, sil := range res {
		remove := false
		for _, f := range q.filters {
			ok, err := f(sil, now)
			if err != nil {
				return nil, err
			}
			if !ok {
				remove = true
				break
			}
		}
		if !remove {
			resf = append(resf, sil)
		}
	}

	return resf, nil
}

// loadSnapshot loads a snapshot generated by Snapshot() into the state.
// Any previous state is wiped.
func (s *Silences) loadSnapshot(r io.Reader) error {
	st := gossipData{}

	for {
		var s pb.MeshSilence
		if _, err := pbutil.ReadDelimited(r, &s); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		st[s.Silence.Id] = &s
	}
	s.mtx.Lock()
	s.st = st
	s.mtx.Unlock()

	return nil
}

// Snapshot writes the full internal state into the writer and returns the number of bytes
// written.
func (s *Silences) Snapshot(w io.Writer) (int, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var n int
	for _, s := range s.st {
		m, err := pbutil.WriteDelimited(w, s)
		if err != nil {
			return n + m, err
		}
		n += m
	}
	return n, nil
}

type gossiper struct {
	*Silences
}

// Gossip implements the mesh.Gossiper interface.
func (g gossiper) Gossip() mesh.GossipData {
	g.mtx.RLock()
	defer g.mtx.RUnlock()

	return g.st.clone()
}

// OnGossip implements the mesh.Gossiper interface.
func (g gossiper) OnGossip(msg []byte) (mesh.GossipData, error) {
	gd, err := decodeGossipData(msg)
	if err != nil {
		return nil, err
	}
	g.mtx.Lock()
	defer g.mtx.Unlock()

	if delta := g.st.mergeDelta(gd); len(delta) > 0 {
		return delta, nil
	}
	return nil, nil
}

// OnGossipBroadcast implements the mesh.Gossiper interface.
func (g gossiper) OnGossipBroadcast(src mesh.PeerName, msg []byte) (mesh.GossipData, error) {
	gd, err := decodeGossipData(msg)
	if err != nil {
		return nil, err
	}
	g.mtx.Lock()
	defer g.mtx.Unlock()

	return g.st.mergeDelta(gd), nil
}

// OnGossipUnicast implements the mesh.Gossiper interface.
// It always panics.
func (g gossiper) OnGossipUnicast(src mesh.PeerName, msg []byte) error {
	panic("not implemented")
}

type gossipData map[string]*pb.MeshSilence

func decodeGossipData(msg []byte) (gossipData, error) {
	gd := gossipData{}
	rd := bytes.NewReader(msg)

	for {
		var s pb.MeshSilence
		if _, err := pbutil.ReadDelimited(rd, &s); err != nil {
			if err == io.EOF {
				break
			}
			return gd, err
		}
		gd[s.Silence.Id] = &s
	}
	return gd, nil
}

// Encode implements the mesh.GossipData interface.
func (gd gossipData) Encode() [][]byte {
	// Split into sub-messages of ~1MB.
	const maxSize = 1024 * 1024

	var (
		buf bytes.Buffer
		res [][]byte
		n   int
	)
	for _, s := range gd {
		m, err := pbutil.WriteDelimited(&buf, s)
		n += m
		if err != nil {
			// TODO(fabxc): log error and skip entry. Or can this really not happen with a bytes.Buffer?
			panic(err)
		}
		if n > maxSize {
			res = append(res, buf.Bytes())
			buf = bytes.Buffer{}
		}
	}
	if buf.Len() > 0 {
		res = append(res, buf.Bytes())
	}
	return res
}

func (gd gossipData) clone() gossipData {
	res := make(gossipData, len(gd))
	for id, s := range gd {
		res[id] = s
	}
	return res
}

// Merge the silence set with gossip data and reutrn a new silence state.
func (gd gossipData) Merge(other mesh.GossipData) mesh.GossipData {
	for id, s := range other.(gossipData) {
		prev, ok := gd[id]
		if !ok {
			gd[id] = s
			continue
		}
		pts, err := ptypes.Timestamp(prev.Silence.UpdatedAt)
		if err != nil {
			panic(err)
		}
		sts, err := ptypes.Timestamp(s.Silence.UpdatedAt)
		if err != nil {
			panic(err)
		}
		if pts.Before(sts) {
			gd[id] = s
		}
	}
	return gd
}

// mergeDelta behaves like Merge but returns a gossipData only
// containing things that have changed.
func (gd gossipData) mergeDelta(od gossipData) gossipData {
	delta := gossipData{}
	for id, s := range od {
		prev, ok := gd[id]
		if !ok {
			gd[id] = s
			delta[id] = s
			continue
		}
		pts, err := ptypes.Timestamp(prev.Silence.UpdatedAt)
		if err != nil {
			panic(err)
		}
		sts, err := ptypes.Timestamp(s.Silence.UpdatedAt)
		if err != nil {
			panic(err)
		}
		if pts.Before(sts) {
			gd[id] = s
			delta[id] = s
		}
	}
	return delta
}

// replaceFile wraps a file that is moved to another filename on closing.
type replaceFile struct {
	*os.File
	filename string
}

func (f *replaceFile) Close() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Rename(f.File.Name(), f.filename)
}

// openReplace opens a new temporary file that is moved to filename on closing.
func openReplace(filename string) (*replaceFile, error) {
	tmpFilename := fmt.Sprintf("%s.%x", filename, uint64(rand.Int63()))

	f, err := os.Create(tmpFilename)
	if err != nil {
		return nil, err
	}

	rf := &replaceFile{
		File:     f,
		filename: filename,
	}
	return rf, nil
}
