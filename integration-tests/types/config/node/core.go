package node

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/segmentio/ksuid"

	commonassets "github.com/smartcontractkit/chainlink-common/pkg/assets"
	"github.com/smartcontractkit/chainlink-testing-framework/blockchain"
	"github.com/smartcontractkit/chainlink-testing-framework/utils/ptr"
	"github.com/smartcontractkit/chainlink/v2/core/chains/evm/assets"
	evmcfg "github.com/smartcontractkit/chainlink/v2/core/chains/evm/config/toml"
	"github.com/smartcontractkit/chainlink/v2/core/config/toml"
	"github.com/smartcontractkit/chainlink/v2/core/services/chainlink"
	"github.com/smartcontractkit/chainlink/v2/core/services/keystore/keys/ethkey"
	"github.com/smartcontractkit/chainlink/v2/core/store/models"
	"github.com/smartcontractkit/chainlink/v2/core/utils"
	"github.com/smartcontractkit/chainlink/v2/core/utils/config"

	"github.com/smartcontractkit/chainlink/integration-tests/actions/vrfv2_actions/vrfv2_constants"
	it_utils "github.com/smartcontractkit/chainlink/integration-tests/utils"
)

func NewBaseConfig() *chainlink.Config {
	return &chainlink.Config{
		Core: toml.Core{
			RootDir: ptr.Ptr("/home/chainlink"),
			Database: toml.Database{
				MaxIdleConns:     ptr.Ptr(int64(20)),
				MaxOpenConns:     ptr.Ptr(int64(40)),
				MigrateOnStartup: ptr.Ptr(true),
			},
			Log: toml.Log{
				Level:       ptr.Ptr(toml.LogLevel(zapcore.DebugLevel)),
				JSONConsole: ptr.Ptr(true),
				File: toml.LogFile{
					MaxSize: ptr.Ptr(utils.FileSize(0)),
				},
			},
			WebServer: toml.WebServer{
				AllowOrigins:   ptr.Ptr("*"),
				HTTPPort:       ptr.Ptr[uint16](6688),
				SecureCookies:  ptr.Ptr(false),
				SessionTimeout: models.MustNewDuration(time.Hour * 999),
				TLS: toml.WebServerTLS{
					HTTPSPort: ptr.Ptr[uint16](0),
				},
				RateLimit: toml.WebServerRateLimit{
					Authenticated:   ptr.Ptr(int64(2000)),
					Unauthenticated: ptr.Ptr(int64(100)),
				},
			},
			Feature: toml.Feature{
				LogPoller:    ptr.Ptr(true),
				FeedsManager: ptr.Ptr(true),
				UICSAKeys:    ptr.Ptr(true),
			},
			P2P: toml.P2P{},
		},
	}
}

type NodeConfigOpt = func(c *chainlink.Config)

func NewConfig(baseConf *chainlink.Config, opts ...NodeConfigOpt) *chainlink.Config {
	for _, opt := range opts {
		opt(baseConf)
	}
	return baseConf
}

func NewConfigFromToml(tomlFile string, opts ...NodeConfigOpt) (*chainlink.Config, error) {
	readFile, err := os.ReadFile(tomlFile)
	if err != nil {
		return nil, err
	}
	var cfg chainlink.Config
	if err != nil {
		return nil, err
	}
	err = config.DecodeTOML(bytes.NewReader(readFile), &cfg)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &cfg, nil
}

func WithOCR1() NodeConfigOpt {
	return func(c *chainlink.Config) {
		c.OCR = toml.OCR{
			Enabled: ptr.Ptr(true),
		}
	}
}

func WithOCR2() NodeConfigOpt {
	return func(c *chainlink.Config) {
		c.OCR2 = toml.OCR2{
			Enabled: ptr.Ptr(true),
		}
	}
}

// Deprecated: P2Pv1 is soon to be fully deprecated
// WithP2Pv1 enables P2Pv1 and disables P2Pv2
func WithP2Pv1() NodeConfigOpt {
	return func(c *chainlink.Config) {
		c.P2P.V1 = toml.P2PV1{
			Enabled:    ptr.Ptr(true),
			ListenIP:   it_utils.MustIP("0.0.0.0"),
			ListenPort: ptr.Ptr[uint16](6690),
		}
		// disabled default
		c.P2P.V2 = toml.P2PV2{Enabled: ptr.Ptr(false)}
	}
}

func WithP2Pv2() NodeConfigOpt {
	return func(c *chainlink.Config) {
		c.P2P.V2 = toml.P2PV2{
			ListenAddresses: &[]string{"0.0.0.0:6690"},
		}
	}
}

func WithTracing() NodeConfigOpt {
	return func(c *chainlink.Config) {
		c.Tracing = toml.Tracing{
			Enabled:         ptr.Ptr(true),
			CollectorTarget: ptr.Ptr("otel-collector:4317"),
			// ksortable unique id
			NodeID:        ptr.Ptr(ksuid.New().String()),
			SamplingRatio: ptr.Ptr(1.0),
			Mode:          ptr.Ptr("unencrypted"),
			Attributes: map[string]string{
				"env": "smoke",
			},
		}
	}
}

func SetChainConfig(
	cfg *chainlink.Config,
	wsUrls,
	httpUrls []string,
	chain blockchain.EVMNetwork,
	forwarders bool,
) {
	if cfg.EVM == nil {
		var nodes []*evmcfg.Node
		for i := range wsUrls {
			node := evmcfg.Node{
				Name:     ptr.Ptr(fmt.Sprintf("node_%d_%s", i, chain.Name)),
				WSURL:    it_utils.MustURL(wsUrls[i]),
				HTTPURL:  it_utils.MustURL(httpUrls[i]),
				SendOnly: ptr.Ptr(false),
			}

			nodes = append(nodes, &node)
		}
		var chainConfig evmcfg.Chain
		if chain.Simulated {
			chainConfig = evmcfg.Chain{
				AutoCreateKey:      ptr.Ptr(true),
				FinalityDepth:      ptr.Ptr[uint32](1),
				MinContractPayment: commonassets.NewLinkFromJuels(0),
			}
		}
		cfg.EVM = evmcfg.EVMConfigs{
			{
				ChainID: utils.NewBig(big.NewInt(chain.ChainID)),
				Chain:   chainConfig,
				Nodes:   nodes,
			},
		}
		if forwarders {
			cfg.EVM[0].Transactions = evmcfg.Transactions{
				ForwardersEnabled: ptr.Ptr(true),
			}
		}
	}
}

func WithPrivateEVMs(networks []blockchain.EVMNetwork) NodeConfigOpt {
	var evmConfigs []*evmcfg.EVMConfig
	for _, network := range networks {
		evmConfigs = append(evmConfigs, &evmcfg.EVMConfig{
			ChainID: utils.NewBig(big.NewInt(network.ChainID)),
			Chain: evmcfg.Chain{
				AutoCreateKey:      ptr.Ptr(true),
				FinalityDepth:      ptr.Ptr[uint32](50),
				MinContractPayment: commonassets.NewLinkFromJuels(0),
				LogPollInterval:    models.MustNewDuration(1 * time.Second),
				HeadTracker: evmcfg.HeadTracker{
					HistoryDepth: ptr.Ptr(uint32(100)),
				},
				GasEstimator: evmcfg.GasEstimator{
					LimitDefault:  ptr.Ptr(uint32(6000000)),
					PriceMax:      assets.GWei(200),
					FeeCapDefault: assets.GWei(200),
				},
			},
			Nodes: []*evmcfg.Node{
				{
					Name:     ptr.Ptr(network.Name),
					WSURL:    it_utils.MustURL(network.URLs[0]),
					HTTPURL:  it_utils.MustURL(network.HTTPURLs[0]),
					SendOnly: ptr.Ptr(false),
				},
			},
		})
	}
	return func(c *chainlink.Config) {
		c.EVM = evmConfigs
	}
}

func WithVRFv2EVMEstimator(addr string) NodeConfigOpt {
	est := assets.GWei(vrfv2_constants.MaxGasPriceGWei)
	return func(c *chainlink.Config) {
		c.EVM[0].KeySpecific = evmcfg.KeySpecificConfig{
			{
				Key: ptr.Ptr(ethkey.EIP55Address(addr)),
				GasEstimator: evmcfg.KeySpecificGasEstimator{
					PriceMax: est,
				},
			},
		}
		c.EVM[0].Chain.GasEstimator = evmcfg.GasEstimator{
			LimitDefault: ptr.Ptr[uint32](3500000),
		}
		c.EVM[0].Chain.Transactions = evmcfg.Transactions{
			MaxQueued: ptr.Ptr[uint32](10000),
		}
	}
}
