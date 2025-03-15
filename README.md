[![main](https://github.com/flowerinthenight/spindle-rs/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/spindle-rs/actions/workflows/main.yml)

## Overview

This crate implements distributed locking using [Cloud Spanner](https://cloud.google.com/spanner/). It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking mechanism. It's a port of [spindle](https://github.com/flowerinthenight/spindle).
