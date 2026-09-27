IF SCHEMA_ID('import_app') IS NULL EXEC('CREATE SCHEMA import_app');
GO
IF OBJECT_ID('import_app.events') IS NULL
CREATE TABLE import_app.events (
    id nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY NONCLUSTERED,
    payload nvarchar(max) NOT NULL CHECK(ISJSON(payload,VALUE)=1)
);
IF OBJECT_ID('import_app.rejections') IS NULL
CREATE TABLE import_app.rejections (
    job_id bigint NOT NULL REFERENCES resume.jobs(id),
    line bigint NOT NULL,
    raw nvarchar(max) NOT NULL,
    reason nvarchar(max) NOT NULL,
    PRIMARY KEY(job_id,line)
);
