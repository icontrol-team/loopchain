"""Consensus (lft) execution and event handling"""
import asyncio
from typing import TYPE_CHECKING, OrderedDict

from lft.consensus import Consensus
from lft.consensus.events import BroadcastDataEvent, BroadcastVoteEvent, ReceiveDataEvent, ReceiveVoteEvent, \
    InitializeEvent, RoundEndEvent, RoundStartEvent
from lft.event import EventSystem, EventRegister
from lft.event.mediators import DelayedEventMediator

from loopchain import utils
from loopchain.blockchain.epoch3 import LoopchainEpoch
from loopchain.blockchain.votes.v1_0 import BlockVoteFactory
from loopchain.channel.channel_property import ChannelProperty
from loopchain.blockchain.types import Hash32

if TYPE_CHECKING:
    from loopchain.blockchain.types import ExternalAddress
    from loopchain.baseservice import BroadcastScheduler
    from lft.consensus.messages.data import DataFactory
    from lft.consensus.messages.vote import VoteFactory, Vote
    from loopchain.blockchain.blocks.v1_0 import Block, BlockFactory
    from loopchain.blockchain.invoke_result import InvokePool


state_hash = Hash32.empty()
receipt_hash = Hash32.empty()


class ConsensusRunner(EventRegister):
    def __init__(self,
                 node_id: 'ExternalAddress',
                 event_system: 'EventSystem',
                 data_factory: 'DataFactory',
                 vote_factory: 'VoteFactory',
                 broadcast_scheduler: 'BroadcastScheduler'):
        super().__init__(event_system.simulator)
        self.broadcast_scheduler = broadcast_scheduler
        self.event_system = event_system
        self.consensus = Consensus(self.event_system, node_id, data_factory, vote_factory)

        # FIXME
        self._block_factory: 'BlockFactory' = data_factory
        self._vote_factory: 'BlockVoteFactory' = vote_factory
        self._db: OrderedDict[int, 'Block'] = OrderedDict()

    def start(self, event: InitializeEvent):
        utils.logger.notice(f"ConsensusRunner start")

        self._genesis_invoke(event)

        self.event_system.start(blocking=False)
        self.event_system.simulator.raise_event(event)

    async def _on_event_broadcast_data(self, event: BroadcastDataEvent):
        # call broadcast block
        utils.logger.notice(f"_on_event_broadcast_data")
        # await self._do_dummy_vote(block=event.data)

    async def _on_event_broadcast_vote(self, vote: 'Vote'):
        # call broadcast vote
        utils.logger.notice(f"_on_event_broadcast_vote")
        pass

    async def _on_event_receive_data(self, event: ReceiveDataEvent):
        utils.logger.notice(f"_on_event_receive_data: {event.data.header.height}")
        await self.consensus.receive_data(event.data)

    async def _on_event_receive_vote(self, event: ReceiveVoteEvent):
        utils.logger.notice(f"_on_event_receive_vote")
        await self.consensus.receive_vote(event.vote)

    async def _on_init_event(self, init_event: InitializeEvent):
        utils.logger.notice(f"_on_init_event")

    # FIXME: Temporary
    def _genesis_invoke(self, event):
        genesis_block = event.data_pool[0]

    # FIXME: Temporary
    async def _on_round_end_event(self, round_end_event: RoundEndEvent):
        utils.logger.notice(f"_on_round_end_event")

        await self._write_block(round_end_event)
        await self._round_start(round_end_event)

    # FIXME: Temporary
    async def _write_block(self, round_end_event):
        utils.logger.notice(f"> EPOCH // ROUND ({round_end_event.epoch_num} // {round_end_event.round_num})")
        utils.logger.notice(f"> Candidate id: {round_end_event.candidate_id}")
        utils.logger.notice(f"> Commit id: {round_end_event.commit_id}")

        if round_end_event.is_success:
            is_genesis_block = round_end_event.commit_id == Hash32.empty()

            if not is_genesis_block:
                utils.logger.notice(f"> ...all data:{self.consensus._data_pool._messages}")
                data = self.consensus._data_pool.get_data(round_end_event.commit_id)
                self._db[data.number] = data
                utils.logger.notice(f"> ...Write Bloc!")

    # FIXME: Temporary
    async def _round_start(self, event: RoundEndEvent):
        epoch1 = LoopchainEpoch(num=1, voters=(ChannelProperty().peer_address,))
        next_round = event.round_num + 1

        round_start_event = RoundStartEvent(
            epoch=epoch1,
            round_num=next_round
        )
        round_start_event.deterministic = False
        mediator = self.event_system.get_mediator(DelayedEventMediator)
        mediator.execute(0.5, round_start_event)

    # FIXME: Temporary
    async def _do_dummy_vote(self, block: 'Block'):
        utils.logger.notice(f"wait for votes...")
        # await asyncio.sleep(1)

        vote = await self._vote_factory.create_dummy_vote(
            data_id=block.header.hash,
            commit_id=block.header.prev_hash,
            epoch_num=block.header.epoch,
            round_num=block.header.round
        )
        utils.logger.notice(f"Generate Vote hash: {vote.hash}")  # TODO: Hash is not generated automatically...
        event = ReceiveVoteEvent(vote)
        # self.event_system.simulator.raise_event(event)
        # await self._on_event_receive_vote(vote)
        event.deterministic = False
        #
        mediator = self.event_system.get_mediator(DelayedEventMediator)
        mediator.execute(0.5, event)

    async def _start_new_round(self):
        utils.logger.notice(f"_start_new_round")

    _handler_prototypes = {
        BroadcastDataEvent: _on_event_broadcast_data,
        BroadcastVoteEvent: _on_event_broadcast_vote,
        ReceiveDataEvent: _on_event_receive_data,
        ReceiveVoteEvent: _on_event_receive_vote,
        InitializeEvent: _on_init_event,
        RoundEndEvent: _on_round_end_event
    }
