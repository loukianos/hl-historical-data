# ADR 0001: QuestDB column types and indexing strategy

- **Status:** Accepted
- **Date:** 2026-02-12
- **Related issue:** Milestone 1 / Issue 6

## Context

The `fills` table will grow quickly and is queried primarily by time range, wallet (`address`), and coin (`coin`).

QuestDB `SYMBOL` columns are dictionary-encoded and can be indexed, which is excellent for low-cardinality and repeatedly filtered fields. But high-cardinality/unbounded fields in `SYMBOL` can create large symbol tables and memory pressure over time.

## Decision

### `fills` table types

- `time`, `block_time`, `local_time`: `TIMESTAMP`
- `block_number`, `oid`, `tid`: `LONG`
- `px`, `sz`, `start_position`, `closed_pnl`, `fee`: `DOUBLE`
- `is_buy`, `is_gaining_inventory`, `crossed`: `BOOLEAN`
- `coin`, `type`, `fee_token`: `SYMBOL`
- `address`: `SYMBOL INDEX`
- `coin`: `SYMBOL INDEX`
- `hash`, `cloid`, `builder_fee`, `builder`: `VARCHAR`

### `fills_quarantine` table types

- Keep payload text columns as `VARCHAR` to avoid symbol-table pollution from malformed/outlier values.
- Keep `reason` as `SYMBOL` (small finite set; useful for grouping/filtering).

## Rationale

- `address` is high-cardinality, but wallet filtering is a primary query path; we accept `SYMBOL` + index for performance.
- `hash` and `cloid` are very high-cardinality and lookup-style fields; storing as `VARCHAR` avoids “symbol explosion”.
- `coin`, `type`, `fee_token` are bounded dimensions and a good fit for `SYMBOL`.
- Indexing `coin` and `address` supports the core query shape:
  - `WHERE time >= ? AND time < ? AND address = ? AND coin = ?`

## Expected impact

- Faster wallet+coin filtering on large ranges due to indexed symbol predicates.
- Lower long-term memory risk versus symbolizing all high-cardinality identifiers.
- Hash-based lookups may require scanning more data than an indexed symbol/hash structure; acceptable for MVP and can be revisited if needed.
