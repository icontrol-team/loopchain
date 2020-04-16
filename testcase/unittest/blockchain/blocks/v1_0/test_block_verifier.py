import datetime
import os
from collections import OrderedDict
from typing import List, Callable

import pytest
from freezegun import freeze_time
from lft.consensus.epoch import EpochPool

from loopchain.baseservice.aging_cache import AgingCache
from loopchain.blockchain import TransactionOutOfTimeBound, BlockVersionNotMatch
from loopchain.blockchain.blocks.v1_0 import BlockBuilder, BlockFactory, Block, BlockVerifier
from loopchain.blockchain.invoke_result import InvokePool, InvokeData
from loopchain.blockchain.transactions import TransactionVersioner
from loopchain.blockchain.transactions import v3
from loopchain.blockchain.types import Hash32, ExternalAddress
from loopchain.blockchain.votes.v1_0 import BlockVote, BlockVoteFactory
from loopchain.crypto.signature import Signer
from loopchain.store.key_value_store import KeyValueStore

epoch_num = 1
round_num = 1
height = 1
timestamp = 1

data_id = Hash32(os.urandom(Hash32.size))
commit_id = Hash32(os.urandom(Hash32.size))

signers = [Signer.from_prikey(os.urandom(32)) for _ in range(4)]
validators = [ExternalAddress.fromhex_address(signer.address) for signer in signers]
validators_hash = Hash32(os.urandom(Hash32.size))
next_validators_hash = Hash32.new()  # Validators not changed

receipt_hash = Hash32(os.urandom(Hash32.size))
state_hash = Hash32(os.urandom(Hash32.size))


class TestBlockVerifier:
    @pytest.fixture
    def block_factory(self, mocker, icon_query) -> BlockFactory:
        # TODO: Temporary mocking...
        tx_queue: AgingCache = mocker.MagicMock(AgingCache)
        db: KeyValueStore = mocker.MagicMock(KeyValueStore)
        tx_versioner = TransactionVersioner()

        invoke_pool: InvokePool = mocker.MagicMock(InvokePool)
        invoke_pool.prepare_invoke.return_value = InvokeData.from_dict(
            epoch_num=epoch_num,
            round_num=round_num,
            query_result=icon_query
        )
        signer: Signer = Signer.new()
        epoch_pool = EpochPool()

        return BlockFactory(epoch_pool, tx_queue, db, tx_versioner, invoke_pool, signer)

    @pytest.fixture
    def invoke_pool(self, icon_invoke):
        invoke_data: InvokeData = InvokeData(
            epoch_num=epoch_num, round_num=round_num,
            added_transactions={},
            validators_hash=validators_hash,
            next_validators_origin={}
        )
        invoke_data.add_invoke_result(invoke_result=icon_invoke)

        invoke_pool = InvokePool()
        invoke_pool.add_message(invoke_data)

        return invoke_pool

    @pytest.mark.asyncio
    @pytest.fixture
    async def prev_votes(self, invoke_pool) -> List[BlockVote]:
        votes = []

        for signer in signers:
            vote_factory = BlockVoteFactory(
                invoke_result_pool=invoke_pool,
                signer=signer
            )
            vote = await vote_factory.create_vote(
                data_id=data_id, commit_id=commit_id, epoch_num=epoch_num, round_num=round_num
            )
            votes.append(vote)

        return votes

    @pytest.fixture
    def build_block(self, txs, prev_votes) -> Callable[..., Block]:
        def _(**kwargs):
            block_builder = BlockBuilder.new("1.0", TransactionVersioner())

            # Required in Interface
            block_builder.height = height
            block_builder.prev_hash = commit_id
            block_builder.signer = signers[0]
            block_builder.transactions = kwargs.get("transactions") or txs

            # Required in 1.0
            block_builder.validators = validators
            block_builder.next_validators = validators
            block_builder.prev_votes = prev_votes
            block_builder.epoch = epoch_num
            block_builder.round = round_num

            # Optional
            block_builder.fixed_timestamp = kwargs.get("timestamp")

            return block_builder.build()

        return _

    @pytest.mark.asyncio
    async def test_verify(self, block_factory: BlockFactory, build_block):
        # GIVEN I have a block
        block: Block = build_block()

        # WHEN I have a BlockVerifier and verify block
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # THEN It shall pass
        await verifier.verify(block)

    @pytest.fixture
    def txs(self, tx_factory) -> OrderedDict:
        transactions = OrderedDict()
        for _ in range(5):
            tx = tx_factory(v3.version)
            transactions[tx.hash] = tx

        return transactions

    @pytest.mark.asyncio
    async def test_verify_tx_outbound(self, block_factory: BlockFactory, prev_votes, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # WHEN I have block and its Txs came from far future
        with freeze_time(datetime.datetime.utcnow() + datetime.timedelta(days=5)):
            transactions = OrderedDict()
            for _ in range(5):
                tx = tx_factory(v3.version)
                transactions[tx.hash] = tx
        block_tx_future: Block = build_block(transactions=transactions)

        # THEN Verification fails
        with pytest.raises(TransactionOutOfTimeBound):
            await verifier.verify(block_tx_future)

        # WHEN I have block and its Txs came from past
        with freeze_time(datetime.datetime.utcnow() - datetime.timedelta(days=5)):
            transactions = OrderedDict()
            for _ in range(5):
                tx = tx_factory(v3.version)
                transactions[tx.hash] = tx
        block_tx_past: Block = build_block(transactions=transactions)

        # WHEN Verification also fails
        with pytest.raises(TransactionOutOfTimeBound):
            await verifier.verify(block_tx_past)

    @pytest.mark.asyncio
    async def test_verify_version_mismatch(self, block_factory: BlockFactory, prev_votes, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have block
        block: Block = build_block()
        assert block.header.version == "1.0"

        # AND Suppose that verifier version is not 1.0
        verifier.version = "0.5"

        # WHEN I try to verify block, THEN It raises
        with pytest.raises(BlockVersionNotMatch):
            await verifier.verify(block)
