package loader

import (
	"database/sql"
	"math/big"
	"testing"

	"github.com/google/uuid"
	"github.com/graph-gophers/dataloader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commontypes "github.com/smartcontractkit/chainlink-common/pkg/types"

	"github.com/smartcontractkit/chainlink/v2/core/chains"
	"github.com/smartcontractkit/chainlink/v2/core/chains/evm/config/toml"
	"github.com/smartcontractkit/chainlink/v2/core/chains/evm/txmgr"
	evmtxmgrmocks "github.com/smartcontractkit/chainlink/v2/core/chains/evm/txmgr/mocks"
	coremocks "github.com/smartcontractkit/chainlink/v2/core/internal/mocks"
	"github.com/smartcontractkit/chainlink/v2/core/internal/testutils"
	"github.com/smartcontractkit/chainlink/v2/core/internal/testutils/evmtest"
	chainlinkmocks "github.com/smartcontractkit/chainlink/v2/core/services/chainlink/mocks"
	"github.com/smartcontractkit/chainlink/v2/core/services/feeds"
	feedsMocks "github.com/smartcontractkit/chainlink/v2/core/services/feeds/mocks"
	"github.com/smartcontractkit/chainlink/v2/core/services/job"
	jobORMMocks "github.com/smartcontractkit/chainlink/v2/core/services/job/mocks"
	"github.com/smartcontractkit/chainlink/v2/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/v2/core/utils"
)

func TestLoader_Chains(t *testing.T) {
	t.Parallel()

	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	one := utils.NewBigI(1)
	chain := toml.EVMConfig{ChainID: one, Chain: toml.Defaults(one)}
	two := utils.NewBigI(2)
	chain2 := toml.EVMConfig{ChainID: two, Chain: toml.Defaults(two)}
	evmORM := evmtest.NewTestConfigs(&chain, &chain2)
	app.On("EVMORM").Return(evmORM)

	batcher := chainBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{"2", "1", "3"})
	results := batcher.loadByIDs(ctx, keys)

	assert.Len(t, results, 3)
	config2, err := chain2.TOMLString()
	require.NoError(t, err)
	want2 := commontypes.ChainStatus{ID: "2", Enabled: true, Config: config2}
	assert.Equal(t, want2, results[0].Data.(commontypes.ChainStatus))
	config1, err := chain.TOMLString()
	require.NoError(t, err)
	want1 := commontypes.ChainStatus{ID: "1", Enabled: true, Config: config1}
	assert.Equal(t, want1, results[1].Data.(commontypes.ChainStatus))
	assert.Nil(t, results[2].Data)
	assert.Error(t, results[2].Error)
	assert.ErrorIs(t, results[2].Error, chains.ErrNotFound)
}

func TestLoader_Nodes(t *testing.T) {
	t.Parallel()

	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	chainID1, chainID2, notAnID := big.NewInt(1), big.NewInt(2), big.NewInt(3)

	genNodeStat := func(id string) commontypes.NodeStatus {
		return commontypes.NodeStatus{
			Name:    "test-node-" + id,
			ChainID: id,
		}
	}
	rcInterops := &chainlinkmocks.FakeRelayerChainInteroperators{Nodes: []commontypes.NodeStatus{
		genNodeStat(chainID2.String()), genNodeStat(chainID1.String()),
	}}

	app.On("GetRelayers").Return(rcInterops)
	batcher := nodeBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{chainID2.String(), chainID1.String(), notAnID.String()})
	found := batcher.loadByChainIDs(ctx, keys)

	require.Len(t, found, 3)
	assert.Equal(t, []commontypes.NodeStatus{genNodeStat(chainID2.String())}, found[0].Data)
	assert.Equal(t, []commontypes.NodeStatus{genNodeStat(chainID1.String())}, found[1].Data)
	assert.Equal(t, []commontypes.NodeStatus{}, found[2].Data)
}

func TestLoader_FeedsManagers(t *testing.T) {
	t.Parallel()

	fsvc := feedsMocks.NewService(t)
	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	mgr1 := feeds.FeedsManager{
		ID:   int64(1),
		Name: "manager 1",
	}
	mgr2 := feeds.FeedsManager{
		ID:   int64(2),
		Name: "manager 2",
	}
	mgr3 := feeds.FeedsManager{
		ID:   int64(3),
		Name: "manager 3",
	}

	fsvc.On("ListManagersByIDs", []int64{3, 1, 2, 5}).Return([]feeds.FeedsManager{
		mgr1, mgr2, mgr3,
	}, nil)
	app.On("GetFeedsService").Return(fsvc)

	batcher := feedsBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2", "5"})
	found := batcher.loadByIDs(ctx, keys)

	require.Len(t, found, 4)
	assert.Equal(t, mgr3, found[0].Data)
	assert.Equal(t, mgr1, found[1].Data)
	assert.Equal(t, mgr2, found[2].Data)
	assert.Nil(t, found[3].Data)
	assert.Error(t, found[3].Error)
	assert.Equal(t, "feeds manager not found", found[3].Error.Error())
}

func TestLoader_JobProposals(t *testing.T) {
	t.Parallel()

	fsvc := feedsMocks.NewService(t)
	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	jp1 := feeds.JobProposal{
		ID:             int64(1),
		FeedsManagerID: int64(3),
		Status:         feeds.JobProposalStatusPending,
	}
	jp2 := feeds.JobProposal{
		ID:             int64(2),
		FeedsManagerID: int64(1),
		Status:         feeds.JobProposalStatusApproved,
	}
	jp3 := feeds.JobProposal{
		ID:             int64(3),
		FeedsManagerID: int64(1),
		Status:         feeds.JobProposalStatusRejected,
	}

	fsvc.On("ListJobProposalsByManagersIDs", []int64{3, 1, 2}).Return([]feeds.JobProposal{
		jp1, jp3, jp2,
	}, nil)
	app.On("GetFeedsService").Return(fsvc)

	batcher := jobProposalBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2"})
	found := batcher.loadByManagersIDs(ctx, keys)

	require.Len(t, found, 3)
	assert.Equal(t, []feeds.JobProposal{jp1}, found[0].Data)
	assert.Equal(t, []feeds.JobProposal{jp3, jp2}, found[1].Data)
	assert.Equal(t, []feeds.JobProposal{}, found[2].Data)
}

func TestLoader_JobRuns(t *testing.T) {
	t.Parallel()

	jobsORM := jobORMMocks.NewORM(t)
	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	run1 := pipeline.Run{ID: int64(1)}
	run2 := pipeline.Run{ID: int64(2)}
	run3 := pipeline.Run{ID: int64(3)}

	jobsORM.On("FindPipelineRunsByIDs", []int64{3, 1, 2}).Return([]pipeline.Run{
		run3, run1, run2,
	}, nil)
	app.On("JobORM").Return(jobsORM)

	batcher := jobRunBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2"})
	found := batcher.loadByIDs(ctx, keys)

	require.Len(t, found, 3)
	assert.Equal(t, run3, found[0].Data)
	assert.Equal(t, run1, found[1].Data)
	assert.Equal(t, run2, found[2].Data)
}

func TestLoader_JobsByPipelineSpecIDs(t *testing.T) {
	t.Parallel()

	t.Run("with out errors", func(t *testing.T) {
		t.Parallel()

		jobsORM := jobORMMocks.NewORM(t)
		app := coremocks.NewApplication(t)
		ctx := InjectDataloader(testutils.Context(t), app)

		job1 := job.Job{ID: int32(2), PipelineSpecID: int32(1)}
		job2 := job.Job{ID: int32(3), PipelineSpecID: int32(2)}
		job3 := job.Job{ID: int32(4), PipelineSpecID: int32(3)}

		jobsORM.On("FindJobsByPipelineSpecIDs", []int32{3, 1, 2}).Return([]job.Job{
			job1, job2, job3,
		}, nil)
		app.On("JobORM").Return(jobsORM)

		batcher := jobBatcher{app}

		keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2"})
		found := batcher.loadByPipelineSpecIDs(ctx, keys)

		require.Len(t, found, 3)
		assert.Equal(t, job3, found[0].Data)
		assert.Equal(t, job1, found[1].Data)
		assert.Equal(t, job2, found[2].Data)
	})

	t.Run("with errors", func(t *testing.T) {
		t.Parallel()

		jobsORM := jobORMMocks.NewORM(t)
		app := coremocks.NewApplication(t)
		ctx := InjectDataloader(testutils.Context(t), app)

		jobsORM.On("FindJobsByPipelineSpecIDs", []int32{3, 1, 2}).Return([]job.Job{}, sql.ErrNoRows)
		app.On("JobORM").Return(jobsORM)

		batcher := jobBatcher{app}

		keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2"})
		found := batcher.loadByPipelineSpecIDs(ctx, keys)

		require.Len(t, found, 1)
		assert.Nil(t, found[0].Data)
		assert.ErrorIs(t, found[0].Error, sql.ErrNoRows)
	})
}

func TestLoader_JobsByExternalJobIDs(t *testing.T) {
	t.Parallel()

	t.Run("with out errors", func(t *testing.T) {
		t.Parallel()

		jobsORM := jobORMMocks.NewORM(t)
		app := coremocks.NewApplication(t)
		ctx := InjectDataloader(testutils.Context(t), app)

		ejID := uuid.New()
		job := job.Job{ID: int32(2), ExternalJobID: ejID}

		jobsORM.On("FindJobByExternalJobID", ejID).Return(job, nil)
		app.On("JobORM").Return(jobsORM)

		batcher := jobBatcher{app}

		keys := dataloader.NewKeysFromStrings([]string{ejID.String()})
		found := batcher.loadByExternalJobIDs(ctx, keys)

		require.Len(t, found, 1)
		assert.Equal(t, job, found[0].Data)
	})
}

func TestLoader_EthTransactionsAttempts(t *testing.T) {
	t.Parallel()

	txStore := evmtxmgrmocks.NewEvmTxStore(t)
	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	ethTxIDs := []int64{1, 2, 3}

	attempt1 := txmgr.TxAttempt{
		ID:   int64(1),
		TxID: ethTxIDs[0],
	}
	attempt2 := txmgr.TxAttempt{
		ID:   int64(1),
		TxID: ethTxIDs[1],
	}

	txStore.On("FindTxAttemptConfirmedByTxIDs", []int64{ethTxIDs[2], ethTxIDs[1], ethTxIDs[0]}).Return([]txmgr.TxAttempt{
		attempt1, attempt2,
	}, nil)
	app.On("TxmStorageService").Return(txStore)

	batcher := ethTransactionAttemptBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{"3", "2", "1"})
	found := batcher.loadByEthTransactionIDs(ctx, keys)

	require.Len(t, found, 3)
	assert.Equal(t, []txmgr.TxAttempt{}, found[0].Data)
	assert.Equal(t, []txmgr.TxAttempt{attempt2}, found[1].Data)
	assert.Equal(t, []txmgr.TxAttempt{attempt1}, found[2].Data)
}

func TestLoader_SpecErrorsByJobID(t *testing.T) {
	t.Parallel()

	t.Run("without errors", func(t *testing.T) {
		t.Parallel()

		jobsORM := jobORMMocks.NewORM(t)
		app := coremocks.NewApplication(t)
		ctx := InjectDataloader(testutils.Context(t), app)

		specErr1 := job.SpecError{ID: int64(2), JobID: int32(1)}
		specErr2 := job.SpecError{ID: int64(3), JobID: int32(2)}
		specErr3 := job.SpecError{ID: int64(4), JobID: int32(3)}

		jobsORM.On("FindSpecErrorsByJobIDs", []int32{3, 1, 2}, mock.Anything).Return([]job.SpecError{
			specErr1, specErr2, specErr3,
		}, nil)
		app.On("JobORM").Return(jobsORM)

		batcher := jobSpecErrorsBatcher{app}

		keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2"})
		found := batcher.loadByJobIDs(ctx, keys)

		require.Len(t, found, 3)
		assert.Equal(t, []job.SpecError{specErr3}, found[0].Data)
		assert.Equal(t, []job.SpecError{specErr1}, found[1].Data)
		assert.Equal(t, []job.SpecError{specErr2}, found[2].Data)
	})

	t.Run("with errors", func(t *testing.T) {
		t.Parallel()

		jobsORM := jobORMMocks.NewORM(t)
		app := coremocks.NewApplication(t)
		ctx := InjectDataloader(testutils.Context(t), app)

		jobsORM.On("FindSpecErrorsByJobIDs", []int32{3, 1, 2}, mock.Anything).Return([]job.SpecError{}, sql.ErrNoRows)
		app.On("JobORM").Return(jobsORM)

		batcher := jobSpecErrorsBatcher{app}

		keys := dataloader.NewKeysFromStrings([]string{"3", "1", "2"})
		found := batcher.loadByJobIDs(ctx, keys)

		require.Len(t, found, 1)
		assert.Nil(t, found[0].Data)
		assert.ErrorIs(t, found[0].Error, sql.ErrNoRows)
	})
}

func TestLoader_loadByEthTransactionID(t *testing.T) {
	t.Parallel()

	txStore := evmtxmgrmocks.NewEvmTxStore(t)
	app := coremocks.NewApplication(t)
	ctx := InjectDataloader(testutils.Context(t), app)

	ethTxID := int64(3)
	ethTxHash := utils.NewHash()

	receipt := txmgr.Receipt{
		ID:     int64(1),
		TxHash: ethTxHash,
	}

	attempt1 := txmgr.TxAttempt{
		ID:       int64(1),
		TxID:     ethTxID,
		Hash:     ethTxHash,
		Receipts: []txmgr.ChainReceipt{txmgr.DbReceiptToEvmReceipt(&receipt)},
	}

	txStore.On("FindTxAttemptConfirmedByTxIDs", []int64{ethTxID}).Return([]txmgr.TxAttempt{
		attempt1,
	}, nil)

	app.On("TxmStorageService").Return(txStore)

	batcher := ethTransactionAttemptBatcher{app}

	keys := dataloader.NewKeysFromStrings([]string{"3"})
	found := batcher.loadByEthTransactionIDs(ctx, keys)

	require.Len(t, found, 1)
	assert.Equal(t, []txmgr.TxAttempt{attempt1}, found[0].Data)
}
