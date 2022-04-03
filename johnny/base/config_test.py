__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import unittest

from johnny.base import config_pb2
from johnny.base import config
from johnny.base.etl import petl


class TestConfig(unittest.TestCase):
    def test_basic(self):
        cfg = config_pb2.Config()
        account = cfg.input.accounts.add()
        account.nickname = "etrade"
        account.logtype.append(config_pb2.Account.LogType.TRANSACTIONS)


if __name__ == "__main__":
    unittest.main()
