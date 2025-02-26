package synchronization

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smartcontractkit/wsrpc"
	"github.com/smartcontractkit/wsrpc/examples/simple/keys"

	"github.com/smartcontractkit/chainlink-common/pkg/services"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	"github.com/smartcontractkit/chainlink/v2/core/services/keystore"
	telemPb "github.com/smartcontractkit/chainlink/v2/core/services/synchronization/telem"
)

type NoopTelemetryIngressClient struct{}

// Start is a no-op
func (NoopTelemetryIngressClient) Start(context.Context) error { return nil }

// Close is a no-op
func (NoopTelemetryIngressClient) Close() error { return nil }

// Send is a no-op
func (NoopTelemetryIngressClient) Send(context.Context, TelemPayload) {}

func (NoopTelemetryIngressClient) HealthReport() map[string]error { return map[string]error{} }
func (NoopTelemetryIngressClient) Name() string                   { return "NoopTelemetryIngressClient" }

// Ready is a no-op
func (NoopTelemetryIngressClient) Ready() error { return nil }

type telemetryIngressClient struct {
	services.StateMachine
	url             *url.URL
	ks              keystore.CSA
	serverPubKeyHex string

	telemClient telemPb.TelemClient
	logging     bool
	lggr        logger.Logger

	wgDone           sync.WaitGroup
	chDone           services.StopChan
	dropMessageCount atomic.Uint32
	chTelemetry      chan TelemPayload
}

// NewTelemetryIngressClient returns a client backed by wsrpc that
// can send telemetry to the telemetry ingress server
func NewTelemetryIngressClient(url *url.URL, serverPubKeyHex string, ks keystore.CSA, logging bool, lggr logger.Logger, telemBufferSize uint) TelemetryService {
	return &telemetryIngressClient{
		url:             url,
		ks:              ks,
		serverPubKeyHex: serverPubKeyHex,
		logging:         logging,
		lggr:            lggr.Named("TelemetryIngressClient"),
		chTelemetry:     make(chan TelemPayload, telemBufferSize),
		chDone:          make(services.StopChan),
	}
}

// Start connects the wsrpc client to the telemetry ingress server
func (tc *telemetryIngressClient) Start(ctx context.Context) error {
	return tc.StartOnce("TelemetryIngressClient", func() error {
		privkey, err := tc.getCSAPrivateKey()
		if err != nil {
			return err
		}

		tc.connect(ctx, privkey)

		return nil
	})
}

// Close disconnects the wsrpc client from the ingress server
func (tc *telemetryIngressClient) Close() error {
	return tc.StopOnce("TelemetryIngressClient", func() error {
		close(tc.chDone)
		tc.wgDone.Wait()
		return nil
	})
}

func (tc *telemetryIngressClient) Name() string {
	return tc.lggr.Name()
}

func (tc *telemetryIngressClient) HealthReport() map[string]error {
	return map[string]error{tc.Name(): tc.Healthy()}
}

func (tc *telemetryIngressClient) connect(ctx context.Context, clientPrivKey []byte) {
	tc.wgDone.Add(1)

	go func() {
		defer tc.wgDone.Done()

		serverPubKey := keys.FromHex(tc.serverPubKeyHex)

		conn, err := wsrpc.DialWithContext(ctx, tc.url.String(), wsrpc.WithTransportCreds(clientPrivKey, serverPubKey), wsrpc.WithLogger(tc.lggr))
		if err != nil {
			if ctx.Err() != nil {
				tc.lggr.Warnw("gave up connecting to telemetry endpoint", "err", err)
			} else {
				tc.lggr.Criticalw("telemetry endpoint dial errored unexpectedly", "err", err)
				tc.SvcErrBuffer.Append(err)
			}
		}
		defer conn.Close()

		// Initialize a new wsrpc client caller
		// This is used to call RPC methods on the server
		if tc.telemClient == nil { // only preset for tests
			tc.telemClient = telemPb.NewTelemClient(conn)
		}

		// Start handler for telemetry
		tc.handleTelemetry()

		// Wait for close
		<-tc.chDone

	}()
}

func (tc *telemetryIngressClient) handleTelemetry() {
	go func() {
		ctx, cancel := tc.chDone.NewCtx()
		defer cancel()
		for {
			select {
			case p := <-tc.chTelemetry:
				// Send telemetry to the ingress server, log any errors
				telemReq := &telemPb.TelemRequest{
					Telemetry:     p.Telemetry,
					Address:       p.ContractID,
					TelemetryType: string(p.TelemType),
					SentAt:        time.Now().UnixNano(),
				}
				_, err := tc.telemClient.Telem(ctx, telemReq)
				if err != nil {
					tc.lggr.Errorf("Could not send telemetry: %v", err)
					continue
				}
				if tc.logging {
					tc.lggr.Debugw("successfully sent telemetry to ingress server", "contractID", p.ContractID, "telemetry", p.Telemetry)
				}
			case <-tc.chDone:
				return
			}
		}
	}()
}

// logBufferFullWithExpBackoff logs messages at
// 1
// 2
// 4
// 8
// 16
// 32
// 64
// 100
// 200
// 300
// etc...
func (tc *telemetryIngressClient) logBufferFullWithExpBackoff(payload TelemPayload) {
	count := tc.dropMessageCount.Add(1)
	if count > 0 && (count%100 == 0 || count&(count-1) == 0) {
		tc.lggr.Warnw("telemetry ingress client buffer full, dropping message", "telemetry", payload.Telemetry, "droppedCount", count)
	}
}

// getCSAPrivateKey gets the client's CSA private key
func (tc *telemetryIngressClient) getCSAPrivateKey() (privkey []byte, err error) {
	// Fetch the client's public key
	keys, err := tc.ks.GetAll()
	if err != nil {
		return privkey, err
	}
	if len(keys) < 1 {
		return privkey, errors.New("CSA key does not exist")
	}

	return keys[0].Raw(), nil
}

// Send sends telemetry to the ingress server using wsrpc if the client is ready.
// Also stores telemetry in a small buffer in case of backpressure from wsrpc,
// throwing away messages once buffer is full
func (tc *telemetryIngressClient) Send(ctx context.Context, telemData []byte, contractID string, telemType TelemetryType) {
	payload := TelemPayload{
		Telemetry:  telemData,
		TelemType:  telemType,
		ContractID: contractID,
	}

	select {
	case tc.chTelemetry <- payload:
		tc.dropMessageCount.Store(0)
	case <-ctx.Done():
		return
	default:
		tc.logBufferFullWithExpBackoff(payload)
	}
}
