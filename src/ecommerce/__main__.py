import logging
from contextlib import suppress

from ecommerce.config import load_config
from ecommerce.consumer import create_cdc_consumer
from ecommerce.logging_config import configure_logging

logger = logging.getLogger(__name__)

def main() -> None:
    configure_logging()

    config = load_config()
    logger.info('configuration loaded')
    consumer = create_cdc_consumer(config)
    logger.info('consumer created')

    with suppress(KeyboardInterrupt):
        consumer.run()


if __name__ == '__main__':
    main()