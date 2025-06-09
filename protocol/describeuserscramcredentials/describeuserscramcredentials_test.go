package describeuserscramcredentials_test

import (
	"testing"

	"github.com/jessekempf/kafka-go/protocol/describeuserscramcredentials"
	"github.com/jessekempf/kafka-go/protocol/prototest"
)

const (
	v0 = 0
)

func TestDescribeUserScramCredentialsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &describeuserscramcredentials.Request{
		Users: []describeuserscramcredentials.RequestUser{
			{
				Name: "foo-1",
			},
		},
	})
}

func TestDescribeUserScramCredentialsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &describeuserscramcredentials.Response{
		ThrottleTimeMs: 500,
		Results: []describeuserscramcredentials.ResponseResult{
			{
				User:         "foo",
				ErrorCode:    1,
				ErrorMessage: "foo-error",
				CredentialInfos: []describeuserscramcredentials.CredentialInfo{
					{
						Mechanism:  2,
						Iterations: 15000,
					},
				},
			},
		},
	})
}
