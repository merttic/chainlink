package zksync

import (
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/smartcontractkit/chainlink-testing-framework/blockchain"
	"github.com/smartcontractkit/chainlink/integration-tests/client"
	"github.com/smartcontractkit/chainlink/integration-tests/contracts"
	"github.com/smartcontractkit/chainlink/integration-tests/gauntlet"
	"math/big"
	"strings"
)

type ZKSyncClient struct {
	GRunner              *gauntlet.GauntletRunner
	LinkAddr             string
	OCRAddr              string
	ContractLoader       contracts.ContractLoader
	LinkContract         contracts.LinkToken
	OCRContract          contracts.OffchainAggregator
	AccessControllerAddr string
	L2RPC                string
	NKeys                []client.NodeKeysBundle
	Transmitters         []string
	Payees               []string
	Signers              []string
	PeerIds              []string
	OcrConfigPubKeys     []string
	Client               blockchain.EVMClient
}

func Setup(L2RPC string, privateKey string, client blockchain.EVMClient) (*ZKSyncClient, error) {
	g, err := gauntlet.Setup(L2RPC, privateKey)
	if err != nil {
		return nil, err
	}

	return &ZKSyncClient{
		GRunner:  g,
		LinkAddr: "",
		OCRAddr:  "",
		L2RPC:    L2RPC,
		Client:   client,
	}, nil

}

func (z *ZKSyncClient) DeployLinkToken() error {
	output, err := z.GRunner.ExecuteCommand([]string{"token:deploy"})
	if err != nil {
		return err
	}
	z.LinkAddr = output.Responses[0].Tx.Address
	return nil
}

func (z *ZKSyncClient) DeployAccessController() error {
	output, err := z.GRunner.ExecuteCommand([]string{"access_controller:deploy"})
	if err != nil {
		return err
	}
	z.AccessControllerAddr = output.Responses[0].Tx.Address
	return nil
}

func (z *ZKSyncClient) DeployOCR(ocrContractValues string) error {
	output, err := z.GRunner.ExecuteCommand([]string{"ocr:deploy", fmt.Sprintf("--input=%s", ocrContractValues)})
	if err != nil {
		return err
	}
	z.OCRAddr = output.Responses[0].Tx.Address
	return nil
}

func (z *ZKSyncClient) AddAccess(ocrAddress string) error {
	_, err := z.GRunner.ExecuteCommand([]string{"access_controller:add_access", fmt.Sprintf("--address=%s", ocrAddress), z.AccessControllerAddr})
	if err != nil {
		return err
	}
	return nil
}

func (z *ZKSyncClient) SetPayees(ocrAddress string, payees []string, transmitters []string) error {
	_, err := z.GRunner.ExecuteCommand([]string{"ocr:set_payees", ocrAddress, fmt.Sprintf("--transmitters=%s", strings.Join(transmitters, ",")), fmt.Sprintf("--payees=%s", strings.Join(payees, ","))})
	if err != nil {
		return err
	}
	return nil
}
func (z *ZKSyncClient) CreateKeys(chainlinkNodes []*client.ChainlinkClient) error {
	var err error

	z.NKeys, _, err = client.CreateNodeKeysBundle(chainlinkNodes, "evm", "280")
	if err != nil {
		return err
	}
	for _, key := range z.NKeys {
		z.PeerIds = append(z.PeerIds, key.PeerID)
		z.OcrConfigPubKeys = append(z.OcrConfigPubKeys, strings.Replace(key.OCRKeys.Data[0].Attributes.OffChainPublicKey, "ocroff_", "", 1))
		z.Transmitters = append(z.Transmitters, strings.Replace(key.EthAddress, "0x", "", 1))
		z.Signers = append(z.Signers, strings.Replace(key.OCRKeys.Data[0].Attributes.OnChainSigningAddress, "ocrsad_", "", 1))
		z.Payees = append(z.Payees, strings.Replace(z.Client.GetDefaultWallet().Address(), "0x", "", 1))
	}

	return nil
}

func (z *ZKSyncClient) SetConfig(ocrAddress, ocrConfigValues string) error {
	_, err := z.GRunner.ExecuteCommand([]string{"ocr:set_config", ocrAddress, fmt.Sprintf("--input=%s", ocrConfigValues)})
	if err != nil {
		return err
	}
	return nil
}

func (z *ZKSyncClient) FundNodes(chainlinkClient blockchain.EVMClient) error {
	for _, key := range z.NKeys {
		log.Info().Str("ZKSync", fmt.Sprintf("Funding ETH to: %s", key.TXKey.Data.ID)).Msg("Executing ZKSync command")
		amount := big.NewFloat(100000000000000000)
		gasEstimates, err := chainlinkClient.EstimateGas(ethereum.CallMsg{})
		if err != nil {
			return err
		}
		fmt.Println(key.TXKey.Data.ID)
		err = chainlinkClient.Fund(key.TXKey.Data.ID, amount, gasEstimates)
		if err != nil {
			log.Info().Str("ERRRRRRRR", fmt.Sprintf("Funding ETH to: %s", key.TXKey.Data.ID)).Msg("Executing ZKSync command")

			return err
		}

		//TO-DO Link funding seems to hang but tx is present on chain
		//err = z.LinkContract.Transfer(key.TXKey.Data.ID, big.NewInt(100000000))
		//if err != nil {
		//	return err
		//}
	}
	return nil
}

func (z *ZKSyncClient) DeployContracts(chainlinkClient blockchain.EVMClient, ocrContractValues *gauntlet.OCRContract, ocrConfigValues *gauntlet.OCRConfig, l zerolog.Logger) error {
	err := z.DeployLinkToken()
	if err != nil {
		return err
	}
	z.ContractLoader, err = contracts.NewContractLoader(chainlinkClient, l)
	if err != nil {
		return err
	}
	z.LinkContract, err = z.ContractLoader.LoadLINKToken(common.HexToAddress(z.LinkAddr).String())

	//err = z.FundNodes(chainlinkClient)
	//if err != nil {
	//	return err
	//}

	err = z.DeployAccessController()
	if err != nil {
		return err
	}

	ocrContractValues.Link = z.LinkAddr
	ocrContractValues.BillingAccessController = z.AccessControllerAddr
	ocrContractValues.RequesterAccessController = z.AccessControllerAddr

	ocrConfigValues.Signers = z.Signers
	ocrConfigValues.Transmitters = z.Transmitters
	ocrConfigValues.OcrConfigPublicKeys = z.OcrConfigPubKeys
	ocrConfigValues.OperatorsPeerIds = strings.Join(z.PeerIds, ",")

	ocrJsonContract, err := ocrContractValues.MarshalOCR()
	if err != nil {
		return err
	}
	err = z.DeployOCR(ocrJsonContract)
	if err != nil {
		return err
	}
	z.OCRContract, err = z.ContractLoader.LoadOcrContract(common.HexToAddress(z.OCRAddr))
	if err != nil {
		return err
	}

	err = z.AddAccess(z.OCRAddr)
	if err != nil {
		return err
	}

	err = z.SetPayees(z.OCRAddr, z.Payees, z.Transmitters)
	if err != nil {
		return err
	}

	ocrJsonConfig, err := ocrConfigValues.MarshalOCRConfig()
	if err != nil {
		return err
	}
	err = z.SetConfig(z.OCRAddr, ocrJsonConfig)
	if err != nil {
		return err
	}

	return nil
}
