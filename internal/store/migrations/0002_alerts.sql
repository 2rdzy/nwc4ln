-- Alerts for the admin: events where the ledger and the node disagree about money,
-- and a shortfall that lasts. One row per kind and key, so a repeat of the same
-- event, or a check that finds it again, adds nothing. Closing hides it for good.
CREATE TABLE alerts (
    id         INTEGER PRIMARY KEY,
    kind       TEXT    NOT NULL,
    key        TEXT    NOT NULL,
    message    TEXT    NOT NULL,
    created_at INTEGER NOT NULL,
    closed_at  INTEGER,
    UNIQUE (kind, key)
);
