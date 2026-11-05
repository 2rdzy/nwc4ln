-- All amounts are integer millisatoshis. All times are Unix seconds.

CREATE TABLE accounts (
    id          INTEGER PRIMARY KEY,
    type        TEXT    NOT NULL CHECK (type IN ('app', 'sub_wallet')),
    name        TEXT    NOT NULL,
    limit_msat  INTEGER CHECK (limit_msat >= 0),
    renewal     TEXT    NOT NULL DEFAULT 'never'
                CHECK (renewal IN ('daily', 'weekly', 'monthly', 'yearly', 'never')),
    expires_at  INTEGER,
    can_pay     INTEGER NOT NULL CHECK (can_pay IN (0, 1)),
    can_invoice INTEGER NOT NULL CHECK (can_invoice IN (0, 1)),
    created_at  INTEGER NOT NULL,
    deleted_at  INTEGER
);

CREATE TABLE connections (
    id            INTEGER PRIMARY KEY,
    account_id    INTEGER NOT NULL REFERENCES accounts (id),
    client_pubkey TEXT    NOT NULL UNIQUE,
    service_key   TEXT    NOT NULL,
    role          TEXT    NOT NULL CHECK (role IN ('full', 'receive_only')),
    last_used_at  INTEGER,
    created_at    INTEGER NOT NULL,
    revoked_at    INTEGER
);

CREATE TABLE requests (
    id             INTEGER PRIMARY KEY,
    nostr_event_id TEXT    NOT NULL UNIQUE,
    connection_id  INTEGER REFERENCES connections (id),
    method         TEXT,
    state          TEXT    NOT NULL,
    created_at     INTEGER NOT NULL
);

-- An empty account_id is the main wallet side of a fund or withdraw.
CREATE TABLE transactions (
    id           INTEGER PRIMARY KEY,
    account_id   INTEGER REFERENCES accounts (id),
    request_id   INTEGER REFERENCES requests (id),
    type         TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    state        TEXT    NOT NULL CHECK (state IN ('pending', 'settled', 'failed')),
    amount_msat  INTEGER NOT NULL CHECK (amount_msat >= 0),
    fee_msat     INTEGER NOT NULL DEFAULT 0 CHECK (fee_msat >= 0),
    reserve_msat INTEGER NOT NULL DEFAULT 0 CHECK (reserve_msat >= 0),
    invoice      TEXT,
    description  TEXT,
    payment_hash TEXT    NOT NULL,
    preimage     TEXT,
    internal     INTEGER NOT NULL DEFAULT 0 CHECK (internal IN (0, 1)),
    created_at   INTEGER NOT NULL,
    settled_at   INTEGER,
    expires_at   INTEGER
);

CREATE INDEX transactions_payment_hash ON transactions (payment_hash);
CREATE INDEX transactions_account ON transactions (account_id, created_at);

CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
