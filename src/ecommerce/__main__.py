from contextlib import suppress

from ecommerce.config import load_config
from ecommerce.consumer import create_cdc_consumer


def main() -> None:
    config = load_config()
    consumer = create_cdc_consumer(config)

    with suppress(KeyboardInterrupt):
        consumer.run()


if __name__ == '__main__':
    main()