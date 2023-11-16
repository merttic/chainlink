package cache

import (
	"context"

	"github.com/smartcontractkit/chainlink/v2/core/services/relay/evm/mercury/wsrpc/pb"
)

var _ Client = &mockClient{}

type mockClient struct {
	resp *pb.LatestReportResponse
	err  error
}

func (m *mockClient) LatestReport(ctx context.Context, req *pb.LatestReportRequest) (resp *pb.LatestReportResponse, err error) {
	return m.resp, m.err
}

func (m *mockClient) ServerURL() string {
	return "mock client url"
}
