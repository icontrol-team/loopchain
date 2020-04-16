from typing import TYPE_CHECKING

from lft.consensus.messages.data import DataVerifier

from loopchain import configure_default as conf
from loopchain import utils
from loopchain.blockchain.exception import BlockVersionNotMatch, TransactionOutOfTimeBound
from loopchain.blockchain.invoke_result import InvokePool, InvokeRequest
from loopchain.blockchain.transactions import TransactionVerifier, TransactionVersioner
from loopchain.crypto.signature import SignVerifier

if TYPE_CHECKING:
    from loopchain.blockchain.blocks.v1_0.block import Block, BlockHeader


class BlockVerifier(DataVerifier):
    version = "1.0"
    tx_versioner = TransactionVersioner()

    def __init__(self, invoke_pool: InvokePool):
        self._invoke_pool: InvokePool = invoke_pool

    async def verify(self, data: 'Block'):
        self._verify_transactions(data)
        self._verify_something(data)
        self._verify_version(data)
        self._verify_signature(data)
        self._do_invoke(data)

    def _verify_transactions(self, block: 'Block'):
        for tx in block.body.transactions.values():
            if not utils.is_in_time_boundary(tx.timestamp, conf.TIMESTAMP_BOUNDARY_SECOND, block.header.timestamp):
                raise TransactionOutOfTimeBound(tx, block.header.timestamp)

            tv = TransactionVerifier.new(tx.version, tx.type(), self.tx_versioner)
            tv.verify(tx)

    def _verify_something(self, block: 'Block'):
        header: 'BlockHeader' = block.header
        if header.timestamp is None:
            raise RuntimeError(f"Block({header.height}, {header.hash.hex()} does not have timestamp.")
        if header.height > 0 and header.prev_hash is None:
            raise RuntimeError(f"Block({header.height}, {header.hash.hex()} does not have prev_hash.")

    def _verify_version(self, block: 'Block'):
        if block.header.version != self.version:
            raise BlockVersionNotMatch(block.header.version, self.version,
                                       f"The block version is incorrect. Block({block.header})")

    def _verify_signature(self, block: 'Block'):
        sign_verifier = SignVerifier.from_address(block.header.peer_id.hex_xx())
        try:
            sign_verifier.verify_hash(block.header.hash, block.header.signature)
        except Exception as e:
            raise RuntimeError(f"Block({block.header.height}, {block.header.hash.hex()}, Invalid Signature {e}")

    def _do_invoke(self, block):
        invoke_request = InvokeRequest.from_block(block=block)
        self._invoke_pool.invoke(
            epoch_num=block.header.epoch,
            round_num=block.header.round,
            invoke_request=invoke_request
        )
        # TODO: No more new_block comparison?

