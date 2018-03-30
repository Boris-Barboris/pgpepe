[![Build Status](https://travis-ci.org/Boris-Barboris/pgpepe.svg?branch=master)](https://travis-ci.org/Boris-Barboris/pgpepe)

# pgpepe
Pgpepe is a library for D that should help you tackle mundane tasks of issuing requests
to Postgres cluster from your vibe-d service. It is dependent on vibe-core library and witten specifically for it.

## Cartoons

![high-level overview](https://raw.githubusercontent.com/Boris-Barboris/pgpepe/master/docs/pgpepe-high-level.png "Overview")

![fast transactions](https://raw.githubusercontent.com/Boris-Barboris/pgpepe/master/docs/pgpepe_fast_tsac_wire.png "Fast transactions on the wire")

![transaction state machine](https://raw.githubusercontent.com/Boris-Barboris/pgpepe/master/docs/pgpepe_tsac_sm.png "Transaction state machine")