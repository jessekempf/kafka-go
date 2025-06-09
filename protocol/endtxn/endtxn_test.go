package endtxn_test

import (
	"testing"

	"github.com/jessekempf/kafka-go/protocol/endtxn"
	"github.com/jessekempf/kafka-go/protocol/prototest"
)

func TestEndTxnRequest(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestRequest(t, version, &endtxn.Request{
			TransactionalID: "transactional-id-1",
			ProducerID:      1,
			ProducerEpoch:   100,
			Committed:       false,
		})
	}
}

func TestEndTxnResponse(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestResponse(t, version, &endtxn.Response{
			ThrottleTimeMs: 1000,
			ErrorCode:      4,
		})
	}
}
