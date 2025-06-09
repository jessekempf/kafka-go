package kafka

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/jessekempf/kafka-go/protocol"
	fetchAPI "github.com/jessekempf/kafka-go/protocol/fetch"
)

// FetchRequest represents a request sent to a kafka broker to retrieve records
// from one or more partitions of one or more topics.
type FetchRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// Size and time limits of the response returned by the broker.
	MinBytes int32
	MaxBytes int32
	MaxWait  time.Duration

	// The isolation level for the request.
	//
	// Defaults to ReadUncommitted.
	//
	// This field requires the kafka broker to support the Fetch API in version
	// 4 or above (otherwise the value is ignored).
	IsolationLevel IsolationLevel

	// Topics, partitions, and offsets to retrieve records from. The offset may be
	// one of the special FirstOffset or LastOffset constants, in which case the
	// request will automatically discover the first or last offset of the
	// partition and submit the request for these.
	Topics []FetchRequestTopic
}

// FetchRequestTopic represents a topic and partitions that should have records retrieved.
type FetchRequestTopic struct {
	// Topic to retrieve records from.
	Topic string

	// Partitions to retrieve records from.
	Partitions []FetchRequestPartition
}

// FetchRequestPartition represents a partition to retrieve records from.
type FetchRequestPartition struct {
	// Partition to retrieve record from.
	Partition int
	// Offset to retrieve.
	Offset int64
	// Size limit on the response for this partition.
	MaxBytes int32
}

// FetchResponse represents a response from a kafka broker to a fetch request.
type FetchResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// An error that may have occurred while attempting to fetch the records.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error

	// Metadata and records returned by the broker, organized on a per-topic basis.
	Topics []FetchResponseTopic
}

// FetchResponseTopic represents metadata nad records returned by the broker for a single topic.
type FetchResponseTopic struct {
	// The topic that the response came from.
	Topic string

	// The partitions that the response came from.
	Partitions []FetchResponsePartition
}

// FetchResponsePartition represents partition metadata and records returned by the broker.
type FetchResponsePartition struct {
	// Partition the response is for.
	Partition int

	// Information about the topic-partition layout returned from the broker.
	//
	// LastStableOffset requires the kafka broker to support the Fetch API in
	// version 4 or above (otherwise the value is zero).
	//
	/// LogStartOffset requires the kafka broker to support the Fetch API in
	// version 5 or above (otherwise the value is zero).
	HighWatermark    int64
	LastStableOffset int64
	LogStartOffset   int64

	// An error that may have occurred while attempting to fetch the records.
	//
	// The error contains both the kafka error code, and an error message
	// returned by the kafka broker. Programs may use the standard errors.Is
	// function to test the error against kafka error codes.
	Error error

	// The set of records returned in the response.
	//
	// The program is expected to call the RecordSet's Close method when it
	// finished reading the records.
	//
	// Note that kafka may return record batches that start at an offset before
	// the one that was requested. It is the program's responsibility to skip
	// the offsets that it is not interested in.
	Records RecordReader
}

// Fetch sends a fetch request to a kafka broker and returns the response.
//
// If the broker returned an invalid response with no topics, an error wrapping
// protocol.ErrNoTopic is returned.
//
// If the broker returned an invalid response with no partitions, an error
// wrapping ErrNoPartitions is returned.
func (c *Client) Fetch(ctx context.Context, req *FetchRequest) (*FetchResponse, error) {
	timeout := c.timeout(ctx, math.MaxInt64)
	maxWait := defaultMaxWait

	if req.MaxWait > 0 {
		maxWait = req.MaxWait
	}

	if maxWait < timeout {
		timeout = maxWait
	}

	needOffsetResolution := make(map[string][]OffsetRequest)
	resolvedOffsets := make(map[string]map[int]PartitionOffsets)

	for _, topic := range req.Topics {
		for _, partition := range topic.Partitions {
			if partition.Offset == FirstOffset || partition.Offset == LastOffset {
				if _, ok := needOffsetResolution[topic.Topic]; !ok {
					needOffsetResolution[topic.Topic] = make([]OffsetRequest, 0, len(topic.Partitions))
				}
				needOffsetResolution[topic.Topic] = append(needOffsetResolution[topic.Topic], OffsetRequest{
					Partition: partition.Partition,
					Timestamp: partition.Offset,
				})
			}
		}
	}

	r, err := c.ListOffsets(ctx, &ListOffsetsRequest{
		Addr:           req.Addr,
		Topics:         map[string][]OffsetRequest{},
		IsolationLevel: req.IsolationLevel,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", err)
	}

	for topic, partitionOffsets := range r.Topics {
		table := make(map[int]PartitionOffsets)

		for _, offsets := range partitionOffsets {
			table[offsets.Partition] = offsets
		}

		resolvedOffsets[topic] = table
	}

	requestTopics := []fetchAPI.RequestTopic{}

	for _, topic := range req.Topics {
		requestPartitions := []fetchAPI.RequestPartition{}

		for _, partition := range topic.Partitions {
			offset := partition.Offset

			switch offset {
			case FirstOffset:
				offset = resolvedOffsets[topic.Topic][partition.Partition].FirstOffset
			case LastOffset:
				offset = resolvedOffsets[topic.Topic][partition.Partition].LastOffset
			}

			maxBytes := req.MaxBytes

			if partition.MaxBytes > 0 {
				maxBytes = partition.MaxBytes
			}

			requestPartitions = append(requestPartitions, fetchAPI.RequestPartition{
				Partition:          int32(partition.Partition),
				CurrentLeaderEpoch: -1,
				FetchOffset:        offset,
				LogStartOffset:     -1,
				PartitionMaxBytes:  maxBytes,
			})
		}

		requestTopics = append(requestTopics, fetchAPI.RequestTopic{
			Topic:      topic.Topic,
			Partitions: requestPartitions,
		})
	}

	m, err := c.roundTrip(ctx, req.Addr, &fetchAPI.Request{
		ReplicaID:      -1,
		MaxWaitTime:    milliseconds(timeout),
		MinBytes:       int32(req.MinBytes),
		MaxBytes:       int32(req.MaxBytes),
		IsolationLevel: int8(req.IsolationLevel),
		SessionID:      -1,
		SessionEpoch:   -1,
		Topics:         requestTopics,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", err)
	}

	res := m.(*fetchAPI.Response)

	if len(res.Topics) == 0 {
		return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", protocol.ErrNoTopic)
	}

	topics := make([]FetchResponseTopic, 0, len(res.Topics))

	for _, topic := range res.Topics {
		if len(topic.Partitions) == 0 {
			return nil, fmt.Errorf("kafka.(*Client).Fetch: %w", protocol.ErrNoPartition)
		}

		partitions := make([]FetchResponsePartition, 0, len(topic.Partitions))

		for _, partition := range topic.Partitions {
			records := partition.RecordSet.Records

			if records == nil {
				records = NewRecordReader()
			}

			partitions = append(partitions, FetchResponsePartition{
				Partition:        int(partition.Partition),
				HighWatermark:    partition.HighWatermark,
				LastStableOffset: partition.LastStableOffset,
				LogStartOffset:   partition.LogStartOffset,
				Error:            makeError(partition.ErrorCode, ""),
				Records:          records,
			})
		}

		topics = append(topics, FetchResponseTopic{
			Topic:      topic.Topic,
			Partitions: partitions,
		})

	}

	return &FetchResponse{
		Throttle: makeDuration(res.ThrottleTimeMs),
		Error:    makeError(res.ErrorCode, ""),
		Topics:   topics,
	}, nil
}

func (req *FetchRequest) maxWait() time.Duration {
	if req.MaxWait > 0 {
		return req.MaxWait
	}
	return defaultMaxWait
}

type fetchRequestV2 struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      []fetchRequestTopicV2
}

func (r fetchRequestV2) size() int32 {
	return 4 + 4 + 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r fetchRequestV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ReplicaID)
	wb.writeInt32(r.MaxWaitTime)
	wb.writeInt32(r.MinBytes)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type fetchRequestTopicV2 struct {
	TopicName  string
	Partitions []fetchRequestPartitionV2
}

func (t fetchRequestTopicV2) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t fetchRequestTopicV2) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type fetchRequestPartitionV2 struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (p fetchRequestPartitionV2) size() int32 {
	return 4 + 8 + 4
}

func (p fetchRequestPartitionV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt64(p.FetchOffset)
	wb.writeInt32(p.MaxBytes)
}

type fetchResponseV2 struct {
	ThrottleTime int32
	Topics       []fetchResponseTopicV2
}

func (r fetchResponseV2) size() int32 {
	return 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r fetchResponseV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(r.ThrottleTime)
	wb.writeArray(len(r.Topics), func(i int) { r.Topics[i].writeTo(wb) })
}

type fetchResponseTopicV2 struct {
	TopicName  string
	Partitions []fetchResponsePartitionV2
}

func (t fetchResponseTopicV2) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t fetchResponseTopicV2) writeTo(wb *writeBuffer) {
	wb.writeString(t.TopicName)
	wb.writeArray(len(t.Partitions), func(i int) { t.Partitions[i].writeTo(wb) })
}

type fetchResponsePartitionV2 struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	MessageSetSize      int32
	MessageSet          messageSet
}

func (p fetchResponsePartitionV2) size() int32 {
	return 4 + 2 + 8 + 4 + p.MessageSet.size()
}

func (p fetchResponsePartitionV2) writeTo(wb *writeBuffer) {
	wb.writeInt32(p.Partition)
	wb.writeInt16(p.ErrorCode)
	wb.writeInt64(p.HighwaterMarkOffset)
	wb.writeInt32(p.MessageSetSize)
	p.MessageSet.writeTo(wb)
}
