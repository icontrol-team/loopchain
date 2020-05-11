"""Consensus (lft) execution and event handling"""

from typing import TYPE_CHECKING

from lft.consensus import Consensus
from lft.consensus.events import BroadcastDataEvent, BroadcastVoteEvent, ReceiveDataEvent, ReceiveVoteEvent, \
    InitializeEvent, RoundEndEvent, RoundStartEvent
from lft.event import EventSystem, EventRegister

from loopchain import utils
from loopchain.blockchain.epoch3 import LoopchainEpoch

if TYPE_CHECKING:
    from loopchain.blockchain.types import ExternalAddress
    from loopchain.baseservice import BroadcastScheduler
    from lft.consensus.messages.data import DataFactory, Data
    from lft.consensus.messages.vote import VoteFactory, Vote


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

    def start(self, event: InitializeEvent):
        utils.logger.notice(f"ConsensusRunner start")
        self.event_system.start(blocking=False)
        self.event_system.simulator.raise_event(event)

    async def _on_event_broadcast_data(self, block: 'Data'):
        # call broadcast block
        utils.logger.notice(f"_on_event_broadcast_data")
        pass

    async def _on_event_broadcast_vote(self, vote: 'Vote'):
        # call broadcast vote
        utils.logger.notice(f"_on_event_broadcast_vote")
        pass

    async def _on_event_receive_data(self, block: 'Data'):
        utils.logger.notice(f"_on_event_receive_data")
        await self.consensus.receive_data(block)

    async def _on_event_receive_vote(self, vote: 'Vote'):
        utils.logger.notice(f"_on_event_receive_vote")
        await self.consensus.receive_vote(vote)

    async def _on_init_event(self, init_event: InitializeEvent):
        utils.logger.notice(f"_on_init_event")

    async def _on_round_end_event(self, round_end_event: RoundEndEvent):
        utils.logger.notice(f"_on_round_end_event")

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
