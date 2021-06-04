__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import unittest

from johnny.base import config_pb2
from johnny.base import config
from johnny.base.etl import petl, Table


class TestConfig(unittest.TestCase):

    def test_MapAccount(self):
        cfg = config_pb2.Config()
        account = cfg.accounts.add()
        account.number = '1234567'
        account.nickname = 'etrade'

        account = cfg.accounts.add()
        account.number = '5555555'
        account.nickname = 'bbroker'

        table = petl.wrap([
            ('account', 'date'),
            ('7654321', '2021-05-14'),
            ('1234567', '2021-05-14'),
            ('1234567', '2021-05-15'),
        ])

        actual = config.MapAccount(cfg, table, 'account')
        self.assertSetEqual(set(actual.values('account')), {'7654321', 'etrade'})


if __name__ == '__main__':
    unittest.main()
