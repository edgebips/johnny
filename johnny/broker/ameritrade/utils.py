"""Common thinkorswim utilities."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import re


def GetAccountNumber(filename: str) -> str:
    """Get the account number."""
    with open(filename, encoding='utf8') as infile:
        line = infile.readline()
        # Note: There is a BOM in the front of the file.
        match = re.search(r"(Account|Position) Statement for (?:\d+\*+)?(\d+)", line)
        assert match, "Could not find account in {}".format(line)
        account = match.group(2)
        anon_account = "x{}".format(account[-4:])
        return anon_account
