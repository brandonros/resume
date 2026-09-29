IF SCHEMA_ID('orders_app') IS NULL EXEC('CREATE SCHEMA orders_app');
GO
IF OBJECT_ID('orders_app.inventory') IS NULL
CREATE TABLE orders_app.inventory (
    sku nvarchar(256) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    available bigint NOT NULL CHECK (available>=0)
);
IF OBJECT_ID('orders_app.orders') IS NULL
CREATE TABLE orders_app.orders (
    [key] nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY NONCLUSTERED,
    sku nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
    quantity bigint NOT NULL CHECK (quantity>0),
    status varchar(16) NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','reserved','rejected','shipped')),
    job_id bigint NOT NULL UNIQUE REFERENCES resume.jobs(id)
);
IF OBJECT_ID('orders_app.shipments') IS NULL
CREATE TABLE orders_app.shipments (
    order_key nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY NONCLUSTERED REFERENCES orders_app.orders([key]),
    receipt nvarchar(100) NOT NULL UNIQUE
);
