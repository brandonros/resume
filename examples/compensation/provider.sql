-- Fake provider: separate connection/database, with its own transactions and idempotency keys.
IF SCHEMA_ID('payment_provider') IS NULL EXEC('CREATE SCHEMA payment_provider');
GO
IF OBJECT_ID('payment_provider.charges') IS NULL
CREATE TABLE payment_provider.charges (
    [key] nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY NONCLUSTERED,
    amount bigint NOT NULL CHECK(amount>0),
    calls bigint NOT NULL DEFAULT 1
);
IF OBJECT_ID('payment_provider.refunds') IS NULL
CREATE TABLE payment_provider.refunds (
    [key] nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY NONCLUSTERED,
    charge_key nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL UNIQUE REFERENCES payment_provider.charges([key]),
    amount bigint NOT NULL CHECK(amount>0),
    calls bigint NOT NULL DEFAULT 1
);
