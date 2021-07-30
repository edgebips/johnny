# General

- Report P/L from equity and options separately (for covered calls)

- 'maxdate' on an Active position should be set to today. Somehow it isn't.
  Urgent.

- Convert all enums to have the same capitalized style.
- Convert 'P' and 'C' to 'PUT' and 'CALL'
- Refactor ZERO, ONE, etc. definitions.

- Add 'Index Option' as instype for CBOE indices.

- Finish support for straight up equities.

- Write clear documentation on how to produce inputs on each platform.

- Chains: Add number of adjustments
- Chains: Infer initial strategy and add column

- Add tags

- Add bottom line with sums of partial selections

- Create a local database, inferring initial positions from differential

- Make this public, finalize license

- Add tgtcost%

- Add beta-weighted deltas to multiple references by group (e.g. energy not on
  SPX)

- Add custom mapping of underlying to whatever custom field (e.g. "base", for
  copper from /HG and FCX).

- Include the first 20 minutes of trading in initial credits

- Fix "days" to round up.


- Compute beta-weighted adjusted values yourself (for better betas).
- Add % BP per trade, should be 3-5%.
- Render total % BP used and available, should be 35%.
- Compute notional equivalent exposure.
- Add correlation matrix between the major asset classes (oil, bonds, stocks, etc.).
- Create a metric of delta, strategy and duration diversification.
- Create a distribution of BPR size over Net Liq, should be 1-2%

- Factor out ZERO in petl

- Add best and worst chains in stats, for stocks, for sharing.


# Simplify and make State-based Processing more Robust

Reduce the following loops and dependencies on positions to a single loop at all:

    /home/blais/p/johnny/johnny/base/chains.py:281:    inventory = collections.defaultdict(lambda: collections.defaultdict(Pos))

      _LinkByOverlapping()


We want to centralize the processing *after* the transactional log and relax the
conditions on the production of a normalized log.

Moreover, we want the positions file to be optional, and to only include
positions that aren't seen at all in the log.

  If you had a position before the beginning of the log, and the positions file
  holds it, its opening has to be synthesized somehow.

  Basically, if you have only a partial window of time of a transactions log, you have to be ready to reconcile

  1. positions opened and closed within the window
  2. positions opened BEFORE the window and closed within the window
  3. positions opened within the window and never closed (or closed after the window, if the window isn't up to now)
  4. positions opened before the window, and never closed during the window (or closed after the window)

  (1) is never a problem.
  (2) will require synthesizing a fake opening trade, and can be detected by their unmatched closing
  (3) is going to result in residual positions leftover in the inventory accumulator after processing the log
  (4) is basically undetectable, except if you have a positions file to reconcile against (you will find positions you didn't expect and for those you need to synthesize opens for those)


# Personal

- Complete categorizing all my own trades


# Bugs

- The tgtinit% of ratio spreads is incorrect


# Config

- Create account name mapping, the account numbers shouldn't appear in anywhere.
  Needs nicknames, which means, needs a configuration file.

- Support account name mapping, the account numbers shouldn't appear in
  anywhere. Needs nicknames, which means, needs a configuration file.

- Finish support for annotations and explicit chains.


# Database

- Implement storing already imported data and partial import — So you don't have
  to scroll endlessly like an idiot in TW.

- Fetch prices at open, make options at zero, should work fine.
  Fetch IVs.


# Ameritrade

- Fix filters from ConsolidateChains that were created to isolate Core
  positions. Make this fully general and able to consume long stock as well.

- Parse positions from the account statement in order to get net_liq, as an
  alternative to the positions download (well, which contains greeks).


# Interactive

- Implement equivalent IB converter for transactions and positions.


# Vanguard

- Implement converter


# Risk

- Add notional risk for all instruments
- Append BPR to the table
- Fetch IVs for each of the positions and compute estimate of 1SD and 2SD risk.


# Stats

- Render the histograms smaller, and in a way that lends a nice rectangle for sharing.

- Add commissions and fees, as amount and % of total



# Bugs

- Untangle SPCE earnings - group by expiration, unless there's a rolling trade
  (with same order id). Split off different expirations for chains.

- What's up with this one?

      210111_111922.RUT

- Fix bugs with multiple underlyings:

      MULTIPLE UNDERLYINGS: x1287.210415_104721.NGM21 {'/NGN21', '/NGM21'}
      MULTIPLE UNDERLYINGS: x1227.210426_150842.ZTM21 {'/ZTU21', '/ZTM21'}
      MULTIPLE UNDERLYINGS: x1227.210326_102451.ZCZ21 {'/ZCN21', '/ZCZ21'}

- pygraphviz: Why do matches connect to other matches? Write unit tests.

- Support ALL positions, don't remove Ledger. Or perhaps make the Ledger core
  positions included. This separation just isn't working properly.


- Why didn't PLTR get lumped into the earnings trade?
- These should be merged:
  210510_142056.PLTR
  210511_093011.PLTR
