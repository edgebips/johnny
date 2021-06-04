# NOTE(blais): We don't bother with this anymore; this is now unused, kept
# around just in case we need to revive the complex parsing of the description.
#
# Only the strategy and underlying need be pulled out of the trade description
# for disambiguation with the "Account Trade History" table.
def _BreakDownTradeDescription(description: str) -> Dict[str, Any]:
    """A complex breaking down of the trade description back into its components."""

    # Pieces of regexps used below in matching the different strategies.
    details = "(?P<instrument>.*)"
    underlying = "(?P<underlying>/?[A-Z0-9]+(?::[A-Z]+)?)"
    underlying2 = "(?P<underlying2>/?[A-Z0-9]+(?::[A-Z]+)?)"
    multiplier = "(?P<multiplier>(?:1/)?[0-9]+)"
    suffix = "(?P<suffix>\([A-Za-z]+\))"
    expdate_equity = "\d{1,2} [A-Z]{3} \d{1,2}(?: (?:\[[A-Z+]\]))?"
    expdate_futures = f"[A-Z]{3} \d{1,2}(?: (?:\(EOM\)))? {underlying}"
    expdatef = f"(?P<expdate>{expdate})?"
    strike = "[0-9.]+"
    pc = "(?:PUT|CALL)"
    putcall = "(?P<putcall>PUT|CALL)"
    putcalls = "(?P<putcalls>PUT/CALL|CALL/PUT)"
    size = "[+-]?[0-9,.]+"

    # NOTE: You could easily handle each of the strategies below and expand to
    # all the options legs, like this:
    #   strikes = [ToDecimal(strike) for strike in submatches.pop('strikes').split('/')]
    #   options = []
    #   for strike in strikes:
    #       option = submatches.copy()
    #       option['strike'] = strike
    #       options.append(option)
    # or
    #    matches['expiration'] = parser.parse(matches['expiration']).date()
    #    matches['strike'] = Decimal(matches['strike'])
    #    matches['multiplier'] = Decimal(matches['multiplier'])
    #    matches['quantity'] = Decimal(matches['quantity'])

    # 'VERTICAL SPY 100 (Weeklys) 8 JAN 21 355/350 PUT'
    match = re.match(f"(?P<strategy>VERTICAL) {underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'IRON CONDOR NFLX 100 (Weeklys) 5 FEB 21 502.5/505/500/497.5 CALL/PUT'
    match = re.match(f"(?P<strategy>IRON CONDOR) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'CONDOR NDX 100 16 APR 21 [AM] 13500/13625/13875/13975 CALL"
    match = re.match(f"(?P<strategy>CONDOR) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # '2/2/1/1 ~IRON CONDOR RUT 100 16 APR 21 [AM] 2230/2250/2150/2055 CALL/PUT'
    match = re.match(f"(?P<size>{size}/{size}/{size}/{size}) (?P<strategy>~IRON CONDOR) "
                     f"{underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # '1/-1/1/-1 CUSTOM SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM]/19 MAR 21/19 MAR 21 3990/3980/4000/4010 CALL/CALL/CALL/CALL @-.80'
    match = re.match(f"(?P<size>{size}/{size}/{size}/{size}) (?P<strategy>CUSTOM) "
                     f"{underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}/{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) "
                     f"(?P<putcalls>{pc}/{pc}/{pc}/{pc})$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'] + "4", 'quantity': quantity, 'symbol': sub['underlying']}

    # '5/-4 CUSTOM SPX 100 16 APR 21 [AM]/16 APR 21 [AM] 3750/3695 PUT/PUT'
    match = re.match(f"(?P<size>{size}/{size}) (?P<strategy>CUSTOM) "
                     f"{underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}) "
                     f"(?P<putcalls>{pc}/{pc})$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'] + "2", 'quantity': quantity, 'symbol': sub['underlying']}

    # 'BUTTERFLY GS 100 (Weeklys) 5 FEB 21 300/295/290 PUT'
    match = re.match(f"(?P<strategy>BUTTERFLY) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'VERT ROLL NDX 100 (Weeklys) 29 JAN 21/22 JAN 21 13250/13275/13250/13275 CALL'
    match = re.match(f"(?P<strategy>VERT ROLL) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}/{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'DIAGONAL SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM] 3990/3995 CALL'
    match = re.match(f"(?P<strategy>DIAGONAL) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}/{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'CALENDAR SPY 100 16 APR 21/19 MAR 21 386 PUT'
    match = re.match(f"(?P<strategy>CALENDAR) {underlying} {multiplier}(?: {suffix})? "
                     f"(?P<expdate>{expdate}/{expdate}) "
                     f"(?P<strikes>{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'STRANGLE NVDA 100 (Weeklys) 1 APR 21 580/520 CALL/PUT'
    match = re.match(f"(?P<strategy>STRANGLE) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}/{strike}) {putcalls}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying']}

    # 'COVERED LIT 100 16 APR 21 64 CALL/LIT'
    match = re.match(f"(?P<strategy>COVERED) {underlying} {multiplier}(?: {suffix})? "
                     f"{expdatef} "
                     f"(?P<strikes>{strike}) {putcall}/{underlying2}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': sub['strategy'], 'quantity': quantity, 'symbol': sub['underlying2']}

    # 'GAMR 100 16 APR 21 100 PUT'  (-> SINGLE)
    match = re.match(f"{underlying} {multiplier}(?: {suffix})? {expdatef} "
                     f"(?P<strikes>{strike}) {putcall}$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'SINGLE', 'quantity': quantity, 'symbol': sub['underlying']}

    # 'EWW'
    match = re.match(f"{underlying}$$", rest)
    if match:
        sub = match.groupdict()
        return {'strategy': 'EQUITY', 'quantity': quantity, 'symbol': sub['underlying']}

    message = "Unknown description: '{}'".format(description)
    raise ValueError(message)









def Risk():
    pass
    #          # TODO(blais): Move this to a risk report.
    #          .addfield('size', lambda r: r.multiplier * r.price * r.quantity))
    #          #.select('size', lambda v: v > 10000))
    # print(table.head(10).lookallstr())
