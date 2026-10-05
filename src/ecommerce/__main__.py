import logging
import signal
from types import FrameType

from ecommerce.config import load_config
from ecommerce.consumer import CdcConsumer, create_cdc_consumer
from ecommerce.logging_config import configure_logging

logger = logging.getLogger(__name__)


def _register_signal_handlers(consumer: CdcConsumer) -> None:
    def handle_signal(
        signum: int,
        _frame: FrameType | None
    ) -> None:
        logger.info(
            'shutdown signal received: %s',
            signal.Signals(signum).name
        )
        consumer.request_stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def main() -> None:
    configure_logging()

    config = load_config()
    logger.info('configuration loaded')

    consumer = create_cdc_consumer(config)
    logger.info('consumer created')

    _register_signal_handlers(consumer)

    consumer.run()


if __name__ == '__main__':
    main()