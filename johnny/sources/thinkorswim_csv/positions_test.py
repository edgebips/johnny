__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import unittest

from johnny.sources.thinkorswim_csv import positions


_EXAMPLE_INSTRUMENTS = [
    (('1/100 JUL 21 /OGN21 1830 CALL', '/GC'), '/GCQ21_OGN21_C1830'),
    (('1/125000 JUN 21 (European) /EUUM21 1.13 PUT', '/6E'), '/6EM21_EUUM21_P1.13'),
    (('1/125000 JUN 21 (European) /EUUM21 1.19 PUT', '/6E'), '/6EM21_EUUM21_P1.19'),
    (('1/125000 JUN 21 (European) /EUUM21 1.22 CALL', '/6E'), '/6EM21_EUUM21_C1.22'),
    (('1/125000 JUN 21 (European) /EUUM21 1.29 CALL', '/6E'), '/6EM21_EUUM21_C1.29'),
    (('1/50 JUL 21 /OZCN21 560 PUT', '/ZC'), '/ZCN21_OZCN21_P560'),
    (('1/50 JUL 21 /OZCN21 610 PUT', '/ZC'), '/ZCN21_OZCN21_P610'),
    (('1/50 JUL 21 /OZCN21 700 CALL', '/ZC'), '/ZCN21_OZCN21_C700'),
    (('1/50 JUL 21 /OZCN21 755 CALL', '/ZC'), '/ZCN21_OZCN21_C755'),
    (('1/50 JUL 21 /OZSN21 1190 PUT', '/ZS'), '/ZSN21_OZSN21_P1190'),
    (('1/50 JUL 21 /OZSN21 1420 PUT', '/ZS'), '/ZSN21_OZSN21_P1420'),
    (('1/50 JUL 21 /OZSN21 1640 CALL', '/ZS'), '/ZSN21_OZSN21_C1640'),
    (('1/50 JUL 21 /OZSN21 1880 CALL', '/ZS'), '/ZSN21_OZSN21_C1880'),
    (('1/5000 JUN 21 /SOM21 25.85 CALL', '/SI'), '/SIN21_SOM21_C25.85'),
    (('100 (Quarterlys) 30 JUN 21 4280 CALL', 'SPX'), 'SPX_210630_C4280'),
    (('100 (Weeklys) 4 JUN 21 4130 CALL', 'SPX'), 'SPX_210604_C4130'),
    (('100 16 DEC 22 270 PUT', 'QQQ'), 'QQQ_221216_P270'),
    (('100 16 DEC 22 3300 PUT', 'SPX'), 'SPX_221216_P3300'),
    (('100 18 JUN 21 185 CALL', 'SI'), 'SI_210618_C185'),
    (('100 18 JUN 21 250 CALL', 'SI'), 'SI_210618_C250'),
    (('100 18 JUN 21 342 CALL', 'QQQ'), 'QQQ_210618_C342'),
    (('100 18 JUN 21 4140 CALL', 'SPX'), 'SPX_210618_C4140'),
    (('100 18 JUN 21 520 PUT', 'TSLA'), 'TSLA_210618_P520'),
    (('100 18 JUN 21 60 PUT', 'SI'), 'SI_210618_P60'),
    (('100 18 JUN 21 620 PUT', 'TSLA'), 'TSLA_210618_P620'),
    (('100 18 JUN 21 74 PUT', 'XOP'), 'XOP_210618_P74'),
    (('100 18 JUN 21 760 CALL', 'TSLA'), 'TSLA_210618_C760'),
    (('100 18 JUN 21 860 CALL', 'TSLA'), 'TSLA_210618_C860'),
    (('100 18 JUN 21 89 CALL', 'XOP'), 'XOP_210618_C89'),
    (('100 18 JUN 21 90 PUT', 'SI'), 'SI_210618_P90'),
    (('100 21 MAY 21 235 CALL', 'VHT'), 'VHT_210521_C235'),
    (('100 21 MAY 21 24 CALL', 'EWS'), 'EWS_210521_C24'),
    (('100 21 MAY 21 26 CALL', 'EWA'), 'EWA_210521_C26'),
    (('100 21 MAY 21 32 CALL', 'EWU'), 'EWU_210521_C32'),
    (('100 21 MAY 21 336 CALL', 'QQQ'), 'QQQ_210521_C336'),
    (('100 21 MAY 21 45 CALL', 'EWW'), 'EWW_210521_C45'),
    (('100 21 MAY 21 60 CALL', 'EWT'), 'EWT_210521_C60'),
    (('100 21 MAY 21 91 CALL', 'EWY'), 'EWY_210521_C91'),
    (('2-Year U.S. Treasury Note Futures,Jun-2021,ETH (prev. /ZTM1)', '/ZT'), '/ZTM21'),
    (('Eurodollar Futures,Dec-2021,ETH (prev. /GEZ1)', '/GE'), '/GEZ21'),
    (('Eurodollar Futures,Jun-2022,ETH (prev. /GEM2)', '/GE'), '/GEM22'),
    (('Gold Futures,Jun-2021, ETH (prev. /GCM1)', '/GC'), '/GCM21'),
    (('INVESCO QQQ TRUST UNIT SER 1 ETF', 'QQQ'), 'QQQ'),
    (('ISHARES INC MSCI AUST ETF', 'EWA'), 'EWA'),
    (('ISHARES INC MSCI MEXICO ETF', 'EWW'), 'EWW'),
    (('ISHARES INC MSCI SINGPOR ETF', 'EWS'), 'EWS'),
    (('ISHARES INC MSCI STH KOR ETF', 'EWY'), 'EWY'),
    (('ISHARES INC MSCI SWEDEN ETF', 'EWD'), 'EWD'),
    (('ISHARES INC MSCI SWITZERLAND ETF', 'EWL'), 'EWL'),
    (('ISHARES INC MSCI TAIWAN ETF', 'EWT'), 'EWT'),
    (('ISHARES TRUST CORE HIGH DV ETF', 'HDV'), 'HDV'),
    (('ISHARES TRUST CORE S&P TTL STK ETF', 'ITOT'), 'ITOT'),
    (('ISHARES TRUST MSCI UK ETF NEW', 'EWU'), 'EWU'),
    (('ISHARES TRUST RUS 2000 GRW ETF', 'IWO'), 'IWO'),
    (('Lean Hog Futures,Aug-2021,ETH (prev. /HEQ1)', '/HE'), '/HEQ21'),
    (('Live Cattle Futures,Aug-2021,ETH (prev. /LEQ1)', '/LE'), '/LEQ21'),
    (('Silver Futures,Jun-2021, ETH (prev. /SIM1)', '/SI'), '/SIM21'),
    (('VANECK VECTORS ETF TRUST VIETNAM ETF', 'VNM'), 'VNM'),
    (('VANGUARD GROWTH ETF', 'VUG'), 'VUG'),
    (('VANGUARD HEALTH CAR ETF', 'VHT'), 'VHT'),
    (('VANGUARD TOTAL STK MKT ETF', 'VTI'), 'VTI'),
]


def test_ParseInstrumentDescription():
    for (description, root), expected in _EXAMPLE_INSTRUMENTS:
        inst = positions.ParseInstrumentDescription(description, root)
        assert expected == str(inst)


if __name__ == '__main__':
    unittest.main()
