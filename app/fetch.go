package main

import (
	"bytes"
	"fmt"
)

type FetchRequest struct {
	maxWaitMs           int32
	minBytes            int32
	maxBytes            int32
	isolationLevel      int8
	sessionID           int32
	sessionEpoch        int32
	topics              []FetchTopic
	forgottenTopicsData []ForgottenTopic
	rackID              string
	taggedFields        map[uint32][]byte
}

func decodeFetchRequest(b []byte) (FetchRequest, error) {
	r := bytes.NewReader(b)
	var req FetchRequest
	var err error
	if req.maxWaitMs, err = readInt[int32](r); err != nil {
		return FetchRequest{}, fmt.Errorf("read max_wait_ms: %w", err)
	}
	if req.minBytes, err = readInt[int32](r); err != nil {
		return FetchRequest{}, fmt.Errorf("read min_bytes: %w", err)
	}
	if req.maxBytes, err = readInt[int32](r); err != nil {
		return FetchRequest{}, fmt.Errorf("read max_bytes: %w", err)
	}
	if req.isolationLevel, err = readInt[int8](r); err != nil {
		return FetchRequest{}, fmt.Errorf("read isolation_level: %w", err)
	}
	if req.sessionID, err = readInt[int32](r); err != nil {
		return FetchRequest{}, fmt.Errorf("read session_id: %w", err)
	}
	if req.sessionEpoch, err = readInt[int32](r); err != nil {
		return FetchRequest{}, fmt.Errorf("read session_epoch: %w", err)
	}
	if req.topics, err = readCompactArray(r, readFetchTopic); err != nil {
		return FetchRequest{}, fmt.Errorf("read topics: %w", err)
	}
	if req.forgottenTopicsData, err = readCompactArray(r, readForgottenTopic); err != nil {
		return FetchRequest{}, fmt.Errorf("read forgotten_topics_data: %w", err)
	}
	if req.rackID, err = readCompactString(r); err != nil {
		return FetchRequest{}, fmt.Errorf("read rack_id: %w", err)
	}
	if req.taggedFields, err = readTaggedFields(r); err != nil {
		return FetchRequest{}, fmt.Errorf("read tagged fields: %w", err)
	}
	return req, nil
}

type FetchTopic struct {
	topicID      [16]byte
	partitions   []FetchPartition
	taggedFields map[uint32][]byte
}

func readFetchTopic(r *bytes.Reader) (FetchTopic, error) {
	var t FetchTopic
	var err error
	if t.topicID, err = readUUID(r); err != nil {
		return FetchTopic{}, fmt.Errorf("read topic_id: %w", err)
	}
	if t.partitions, err = readCompactArray(r, readFetchPartition); err != nil {
		return FetchTopic{}, fmt.Errorf("read partitions: %w", err)
	}
	if t.taggedFields, err = readTaggedFields(r); err != nil {
		return FetchTopic{}, fmt.Errorf("read tagged fields: %w", err)
	}
	return t, nil
}

type FetchPartition struct {
	partition          int32
	currentLeaderEpoch int32
	fetchOffset        int64
	lastFetchedEpoch   int32
	logStartOffset     int64
	partitionMaxBytes  int32
	taggedFields       map[uint32][]byte
}

func readFetchPartition(r *bytes.Reader) (FetchPartition, error) {
	var p FetchPartition
	var err error
	if p.partition, err = readInt[int32](r); err != nil {
		return FetchPartition{}, fmt.Errorf("read partition: %w", err)
	}
	if p.currentLeaderEpoch, err = readInt[int32](r); err != nil {
		return FetchPartition{}, fmt.Errorf("read current_leader_epoch: %w", err)
	}
	if p.fetchOffset, err = readInt[int64](r); err != nil {
		return FetchPartition{}, fmt.Errorf("read fetch_offset: %w", err)
	}
	if p.lastFetchedEpoch, err = readInt[int32](r); err != nil {
		return FetchPartition{}, fmt.Errorf("read last_fetched_epoch: %w", err)
	}
	if p.logStartOffset, err = readInt[int64](r); err != nil {
		return FetchPartition{}, fmt.Errorf("read log_start_offset: %w", err)
	}
	if p.partitionMaxBytes, err = readInt[int32](r); err != nil {
		return FetchPartition{}, fmt.Errorf("read partition_max_bytes: %w", err)
	}
	if p.taggedFields, err = readTaggedFields(r); err != nil {
		return FetchPartition{}, fmt.Errorf("read tagged fields: %w", err)
	}
	return p, nil
}

type ForgottenTopic struct {
	topicID      [16]byte
	partitions   []int32
	taggedFields map[uint32][]byte
}

func readForgottenTopic(r *bytes.Reader) (ForgottenTopic, error) {
	var t ForgottenTopic
	var err error
	if t.topicID, err = readUUID(r); err != nil {
		return ForgottenTopic{}, fmt.Errorf("read topic_id: %w", err)
	}
	if t.partitions, err = readCompactArray(r, readInt[int32]); err != nil {
		return ForgottenTopic{}, fmt.Errorf("read partitions: %w", err)
	}
	if t.taggedFields, err = readTaggedFields(r); err != nil {
		return ForgottenTopic{}, fmt.Errorf("read tagged fields: %w", err)
	}
	return t, nil
}

type FetchResponse struct {
	throttleTimeMs int32
	errorCode      int16
	sessionID      int32
	responses      []FetchableTopicResponse
	taggedFields   map[uint32][]byte
}

func (r FetchResponse) AppendTo(b []byte) []byte {
	b = appendInt32(b, r.throttleTimeMs)
	b = appendInt16(b, r.errorCode)
	b = appendInt32(b, r.sessionID)
	b = appendCompactArray(b, r.responses)
	b = appendTaggedFields(b, r.taggedFields)
	return b
}

type FetchableTopicResponse struct {
	topicID      [16]byte
	partitions   []PartitionData
	taggedFields map[uint32][]byte
}

func (t FetchableTopicResponse) AppendTo(b []byte) []byte {
	b = appendUUID(b, t.topicID)
	b = appendCompactArray(b, t.partitions)
	b = appendTaggedFields(b, t.taggedFields)
	return b
}

type PartitionData struct {
	partitionIndex       int32
	errorCode            int16
	highWatermark        int64
	lastStableOffset     int64
	logStartOffset       int64
	abortedTransactions  []AbortedTransaction
	preferredReadReplica int32
	records              []byte
	taggedFields         map[uint32][]byte
}

func (p PartitionData) AppendTo(b []byte) []byte {
	b = appendInt32(b, p.partitionIndex)
	b = appendInt16(b, p.errorCode)
	b = appendInt64(b, p.highWatermark)
	b = appendInt64(b, p.lastStableOffset)
	b = appendInt64(b, p.logStartOffset)
	b = appendCompactArray(b, p.abortedTransactions)
	b = appendInt32(b, p.preferredReadReplica)
	b = appendCompactNullableBytes(b, p.records)
	b = appendTaggedFields(b, p.taggedFields)
	return b
}

type AbortedTransaction struct {
	producerID   int64
	firstOffset  int64
	taggedFields map[uint32][]byte
}

func (a AbortedTransaction) AppendTo(b []byte) []byte {
	b = appendInt64(b, a.producerID)
	b = appendInt64(b, a.firstOffset)
	b = appendTaggedFields(b, a.taggedFields)
	return b
}

func (b *broker) handleFetch(req Request) (ResponseBody, error) {
	reqBody, err := decodeFetchRequest(req.body)
	if err != nil {
		return nil, fmt.Errorf("decode fetch request: %w", err)
	}

	resp := FetchResponse{
		errorCode: errorCodeNone,
		responses: make([]FetchableTopicResponse, 0, len(reqBody.topics)),
	}

	for _, t := range reqBody.topics {
		tr := FetchableTopicResponse{topicID: t.topicID}
		tr.partitions = make([]PartitionData, len(t.partitions))
		for i, p := range t.partitions {
			tr.partitions[i] = PartitionData{
				partitionIndex:       p.partition,
				errorCode:            errorCodeUnknownTopicID,
				highWatermark:        -1,
				lastStableOffset:     -1,
				logStartOffset:       -1,
				preferredReadReplica: -1,
			}
		}
		resp.responses = append(resp.responses, tr)
	}

	return resp, nil
}
