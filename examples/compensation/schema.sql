IF SCHEMA_ID('compensation_app') IS NULL EXEC('CREATE SCHEMA compensation_app');
GO
IF OBJECT_ID('compensation_app.inventory') IS NULL
CREATE TABLE compensation_app.inventory (
    sku nvarchar(256) PRIMARY KEY,
    available bigint NOT NULL CHECK(available>=0)
);
IF OBJECT_ID('compensation_app.orders') IS NULL
CREATE TABLE compensation_app.orders (
    job_id bigint PRIMARY KEY REFERENCES resume.jobs(id),
    quantity bigint NOT NULL CHECK(quantity>0),
    state varchar(16) NOT NULL CHECK(state IN ('reserved','compensated')),
    released bit NOT NULL DEFAULT 0
);
