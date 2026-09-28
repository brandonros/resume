-- SQL Server 2022+ (compatibility level 160). Re-runnable: Schema.InstallAsync applies it
-- in one transaction under an application lock. Migrations are forward-only; never edit
-- a released version block, append a new one and bump Schema.Version.
-- Procedures join an explicit caller transaction or commit their own transaction.
-- JSON supplied directly to submit must use the same canonical form as the C# API.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
SET NUMERIC_ROUNDABORT OFF;
GO
IF SCHEMA_ID(N'resume') IS NULL EXEC(N'CREATE SCHEMA resume');
GO
IF OBJECT_ID(N'resume.schema_version', N'U') IS NULL
BEGIN
    CREATE TABLE resume.schema_version (
        singleton bit NOT NULL PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
        version int NOT NULL
    );
    -- Installs that predate versioning already hold the version 1 tables.
    INSERT resume.schema_version(version) VALUES(CASE WHEN OBJECT_ID(N'resume.jobs', N'U') IS NULL THEN 0 ELSE 1 END);
END;
GO
IF (SELECT version FROM resume.schema_version) > 2
    THROW 50006, 'Database schema is newer than this library', 1;
GO
IF (SELECT version FROM resume.schema_version) < 1
BEGIN
    CREATE TABLE resume.jobs (
        id bigint IDENTITY PRIMARY KEY,
        workflow nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK (DATALENGTH(workflow) > 0),
        [key] nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK (DATALENGTH([key]) > 0),
        input nvarchar(max) NOT NULL CHECK (ISJSON(input, VALUE) = 1),
        parent_id bigint NULL REFERENCES resume.jobs(id),
        output nvarchar(max) NULL CHECK (output IS NULL OR ISJSON(output, VALUE) = 1),
        attempt bigint NOT NULL DEFAULT 0,
        failures bigint NOT NULL DEFAULT 0,
        leased bit NOT NULL DEFAULT 0,
        -- NULL represents waiting for a child, not a runnable job.
        available_at datetime2(7) NULL DEFAULT SYSUTCDATETIME(),
        completed bit NOT NULL DEFAULT 0,
        paused bit NOT NULL DEFAULT 0,
        last_error nvarchar(max) NULL,
        saga_phase varchar(16) NULL CHECK (saga_phase IN ('forward','compensating','completed','compensated')),
        saga_position int NULL,
        saga_result nvarchar(max) NULL CHECK (saga_result IS NULL OR ISJSON(saga_result, VALUE) = 1),
        -- Hash exact UTF-16 bytes: SQL string equality otherwise ignores trailing spaces.
        dedup AS CONVERT(binary(32), HASHBYTES('SHA2_256', CONVERT(nvarchar(max), DATALENGTH(workflow)) + N':' + workflow + [key])) PERSISTED,
        CONSTRAINT jobs_dedup UNIQUE (dedup)
    );
    CREATE INDEX jobs_ready ON resume.jobs(workflow, available_at, id) WHERE completed = 0 AND paused = 0;
    CREATE INDEX jobs_parent ON resume.jobs(parent_id);
    CREATE TABLE resume.steps (
        job_id bigint NOT NULL REFERENCES resume.jobs(id),
        [key] nvarchar(768) COLLATE Latin1_General_100_BIN2 NOT NULL CHECK (DATALENGTH([key]) > 0),
        position int NOT NULL CHECK (position >= 0),
        once bit NOT NULL,
        compensation nvarchar(512) COLLATE Latin1_General_100_BIN2 NULL CHECK (DATALENGTH(compensation) > 0),
        is_compensation bit NOT NULL DEFAULT 0,
        -- SQL NULL is unresolved; the JSON text 'null' is a completed output.
        output nvarchar(max) NULL CHECK (output IS NULL OR ISJSON(output, VALUE) = 1),
        key_hash AS CONVERT(binary(32), HASHBYTES('SHA2_256', [key])) PERSISTED,
        PRIMARY KEY(job_id, position),
        UNIQUE(job_id, key_hash)
    );
    UPDATE resume.schema_version SET version = 1;
END;
GO
IF (SELECT version FROM resume.schema_version) < 2
BEGIN
    ALTER TABLE resume.jobs ADD lease_ms int NOT NULL CONSTRAINT jobs_lease_ms DEFAULT 60000
        CONSTRAINT jobs_lease_ms_range CHECK (lease_ms BETWEEN 1000 AND 86400000);
    UPDATE resume.schema_version SET version = 2;
END;
GO
CREATE OR ALTER VIEW resume.job_status AS
SELECT id, workflow, [key], attempt,
    CASE WHEN completed = 1 THEN 'completed' WHEN paused = 1 THEN 'paused'
         WHEN leased = 1 AND available_at > SYSUTCDATETIME() THEN 'running'
         WHEN leased = 1 THEN 'lease_expired' WHEN available_at IS NULL THEN 'waiting'
         WHEN available_at > SYSUTCDATETIME() THEN 'scheduled' ELSE 'ready' END AS status,
    available_at, last_error, parent_id, failures, saga_phase, saga_result, output
FROM resume.jobs;
GO
CREATE OR ALTER VIEW resume.unresolved_steps AS
SELECT j.id AS job_id, j.workflow, j.[key] AS job_key, j.attempt, j.status,
    j.available_at, j.last_error, s.[key] AS step_key, s.position
FROM resume.job_status j JOIN resume.steps s ON s.job_id = j.id
WHERE s.once = 1 AND s.output IS NULL;
GO
CREATE OR ALTER FUNCTION resume.after_delay(@seconds float) RETURNS datetime2(7) AS
BEGIN
    IF @seconds IS NULL OR @seconds < 0 OR @seconds >= DATEDIFF_BIG(SECOND, SYSUTCDATETIME(), CONVERT(datetime2, '9999-12-31')) RETURN NULL;
    RETURN DATEADD(MILLISECOND, CONVERT(int, (@seconds - FLOOR(@seconds / 86400) * 86400) * 1000),
        DATEADD(DAY, CONVERT(int, FLOOR(@seconds / 86400)), SYSUTCDATETIME()));
END;
GO
CREATE OR ALTER PROCEDURE resume.submit @workflow nvarchar(max), @key nvarchar(max), @input nvarchar(max), @available_at datetime2(7) = NULL, @parent_id bigint = NULL AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF @workflow IS NULL OR DATALENGTH(@workflow) NOT BETWEEN 2 AND 256 OR @key IS NULL OR DATALENGTH(@key) NOT BETWEEN 2 AND 1024 OR @input IS NULL OR ISJSON(@input, VALUE) <> 1
            THROW 50003, 'Invalid workflow, key, or JSON input', 1;
        DECLARE @hash binary(32) = HASHBYTES('SHA2_256', CONVERT(nvarchar(max), DATALENGTH(@workflow)) + N':' + @workflow + @key);
        DECLARE @id bigint, @saved nvarchar(max), @parent bigint, @w nvarchar(128), @k nvarchar(512);
        SELECT @id=id, @saved=input, @parent=parent_id, @w=workflow, @k=[key]
        FROM resume.jobs WITH (UPDLOCK, HOLDLOCK) WHERE dedup=@hash;
        IF @id IS NULL
        BEGIN
            INSERT resume.jobs(workflow,[key],input,parent_id,available_at) VALUES(@workflow,@key,@input,@parent_id,COALESCE(@available_at,SYSUTCDATETIME()));
            SET @id = SCOPE_IDENTITY();
        END
        ELSE IF @saved COLLATE Latin1_General_100_BIN2 <> @input COLLATE Latin1_General_100_BIN2 OR DATALENGTH(@saved) <> DATALENGTH(@input)
            OR ISNULL(@parent,-1) <> ISNULL(@parent_id,-1)
            OR @w COLLATE Latin1_General_100_BIN2 <> @workflow COLLATE Latin1_General_100_BIN2 OR DATALENGTH(@w) <> DATALENGTH(@workflow)
            OR @k COLLATE Latin1_General_100_BIN2 <> @key COLLATE Latin1_General_100_BIN2 OR DATALENGTH(@k) <> DATALENGTH(@key)
            THROW 50005, 'Idempotency key has different input or parent', 1;
        SELECT @id AS id;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.claim @workflow nvarchar(max), @lease_ms int = 60000 AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF @lease_ms IS NULL OR @lease_ms NOT BETWEEN 1000 AND 86400000 THROW 50003, 'Lease must be between 1 second and 1 day', 1;
        ;WITH candidate AS (
            SELECT TOP(1) * FROM resume.jobs WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK)
            WHERE workflow=@workflow COLLATE Latin1_General_100_BIN2 AND DATALENGTH(workflow)=DATALENGTH(@workflow)
                AND completed=0 AND paused=0 AND available_at<=SYSUTCDATETIME()
            ORDER BY available_at,id
        )
        UPDATE candidate SET attempt=attempt+1, leased=1, lease_ms=@lease_ms, available_at=DATEADD(MILLISECOND,@lease_ms,SYSUTCDATETIME()) OUTPUT inserted.*;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
-- Claims one job by id; READPAST returns nothing while another session holds its row.
CREATE OR ALTER PROCEDURE resume.claim_job @workflow nvarchar(max), @id bigint, @lease_ms int = 60000 AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF @lease_ms IS NULL OR @lease_ms NOT BETWEEN 1000 AND 86400000 THROW 50003, 'Lease must be between 1 second and 1 day', 1;
        UPDATE j SET attempt=attempt+1, leased=1, lease_ms=@lease_ms, available_at=DATEADD(MILLISECOND,@lease_ms,SYSUTCDATETIME()) OUTPUT inserted.*
        FROM resume.jobs j WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK)
        WHERE id=@id AND workflow=@workflow COLLATE Latin1_General_100_BIN2 AND DATALENGTH(workflow)=DATALENGTH(@workflow)
            AND completed=0 AND paused=0 AND available_at<=SYSUTCDATETIME();
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.begin_step @id bigint, @attempt bigint AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        UPDATE resume.jobs SET available_at=DATEADD(MILLISECOND,lease_ms,SYSUTCDATETIME())
        WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0 AND available_at>SYSUTCDATETIME();
        IF @@ROWCOUNT=0 THROW 50001, 'Job claim is no longer valid', 1;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.start_step @id bigint, @attempt bigint, @key nvarchar(max), @position int, @once bit, @compensation nvarchar(max) = NULL, @is_compensation bit = 0 AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        EXEC resume.begin_step @id, @attempt;
        IF @key IS NULL OR DATALENGTH(@key) NOT BETWEEN 2 AND 1536 OR @position IS NULL OR @position<0 OR @once IS NULL OR @is_compensation IS NULL
            OR (@compensation IS NOT NULL AND DATALENGTH(@compensation) NOT BETWEEN 2 AND 1024)
            THROW 50003, 'Invalid step registration', 1;
        DECLARE @phase varchar(16), @boundary int, @expected nvarchar(768), @hash binary(32)=HASHBYTES('SHA2_256',@key);
        SELECT @phase=saga_phase, @boundary=saga_position FROM resume.jobs WHERE id=@id;
        IF @is_compensation=1
        BEGIN
            IF ISNULL(@phase,'')<>'compensating' OR @once=1 OR @compensation IS NOT NULL OR @position<@boundary
                THROW 50002, 'Invalid compensation state', 1;
            SELECT @expected=CONCAT(N'$undo:',position,N':',compensation) FROM resume.steps WHERE job_id=@id AND compensation IS NOT NULL
            ORDER BY position DESC OFFSET (@position-@boundary) ROWS FETCH NEXT 1 ROW ONLY;
            IF @expected IS NULL OR HASHBYTES('SHA2_256',@expected)<>@hash
                THROW 50002, 'Compensations must follow reverse step order', 1;
        END
        ELSE IF @phase='compensating' OR LEFT(@key,6)=N'$undo:' THROW 50002, 'Forward steps cannot run during compensation', 1;
        IF @compensation IS NOT NULL AND ISNULL(@phase,'')<>'forward' THROW 50002, 'Compensation registration requires an active saga', 1;
        IF EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND (key_hash=@hash OR position=@position)
            AND (key_hash<>@hash OR [key] COLLATE Latin1_General_100_BIN2 <> @key COLLATE Latin1_General_100_BIN2
            OR DATALENGTH([key])<>DATALENGTH(@key) OR position<>@position OR once<>@once OR is_compensation<>@is_compensation
            OR ISNULL(HASHBYTES('SHA2_256',compensation),0x)<>ISNULL(HASHBYTES('SHA2_256',@compensation),0x)))
            THROW 50002, 'Step history differs', 1;
        IF EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND output IS NULL AND position<=@position)
            THROW 50002, 'An earlier step outcome is unknown; inspect before proceeding', 1;
        IF NOT EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND key_hash=@hash)
            INSERT resume.steps(job_id,[key],position,once,compensation,is_compensation) VALUES(@id,@key,@position,@once,@compensation,@is_compensation);
        SELECT output FROM resume.steps WHERE job_id=@id AND key_hash=@hash;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.save_step @id bigint, @attempt bigint, @key nvarchar(max), @output nvarchar(max) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF NOT EXISTS(SELECT 1 FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0)
            THROW 50001, 'Job claim is no longer valid', 1;
        IF @output IS NULL OR ISJSON(@output,VALUE)<>1 THROW 50003, 'Output must be JSON, not SQL NULL', 1;
        DECLARE @hash binary(32)=HASHBYTES('SHA2_256',@key);
        UPDATE resume.steps SET output=@output WHERE job_id=@id AND key_hash=@hash AND output IS NULL;
        IF NOT EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND key_hash=@hash) THROW 50002, 'Step has not started', 1;
        SELECT output FROM resume.steps WHERE job_id=@id AND key_hash=@hash;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
-- A step_once action reported that its effect definitely did not happen: forget the marker.
CREATE OR ALTER PROCEDURE resume.discard_step @id bigint, @attempt bigint, @key nvarchar(max) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF NOT EXISTS(SELECT 1 FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0)
            THROW 50001, 'Job claim is no longer valid', 1;
        DELETE s FROM resume.steps AS s
        WHERE s.job_id=@id AND s.key_hash=HASHBYTES('SHA2_256',@key) AND s.once=1 AND s.output IS NULL
            AND NOT EXISTS(SELECT 1 FROM resume.steps AS later WHERE later.job_id=@id AND later.position>s.position);
        IF @@ROWCOUNT=0 THROW 50002, 'Only the latest unresolved step_once can be discarded', 1;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.finish @id bigint, @attempt bigint, @error nvarchar(max) = NULL, @position int = 0, @retry_after_seconds float = NULL, @output nvarchar(max) = NULL AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @ready datetime2=resume.after_delay(COALESCE(@retry_after_seconds,0)), @parent bigint, @locked bigint;
        IF @ready IS NULL THROW 50003, 'Invalid retry delay', 1;
        -- Parent before child: wait_for uses the same order. PostgreSQL's MVCC read
        -- could avoid this cycle; SQL Server locking READ COMMITTED cannot.
        SELECT @parent=parent_id FROM resume.jobs WHERE id=@id;
        IF @error IS NULL AND @parent IS NOT NULL
            SELECT @locked=id FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@parent;
        IF NOT EXISTS(SELECT 1 FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0 AND available_at>SYSUTCDATETIME())
            THROW 50001, 'Job claim is no longer valid', 1;
        IF @error IS NULL AND EXISTS(SELECT 1 FROM resume.jobs WHERE id=@id AND saga_phase IN ('forward','compensating'))
            THROW 50002, 'Cannot complete an unfinished saga', 1;
        IF @error IS NULL AND (@position IS NULL OR @position<0 OR EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND (output IS NULL OR position>=@position)))
            THROW 50002, 'Cannot complete: unresolved or omitted steps', 1;
        UPDATE resume.jobs SET completed=CASE WHEN @error IS NULL THEN 1 ELSE 0 END, leased=0,last_error=@error,
            output=CASE WHEN @error IS NULL THEN COALESCE(@output,N'null') ELSE NULL END,
            failures=failures+CASE WHEN @error IS NULL THEN 0 ELSE 1 END,
            paused=CASE WHEN @error IS NOT NULL AND @retry_after_seconds IS NULL THEN 1 ELSE 0 END, available_at=@ready
        WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0 AND available_at>SYSUTCDATETIME();
        IF @@ROWCOUNT=0 THROW 50001, 'Job claim is no longer valid', 1;
        IF @error IS NULL AND @parent IS NOT NULL
            UPDATE resume.jobs SET available_at=SYSUTCDATETIME() WHERE id=@parent AND leased=0 AND completed=0 AND paused=0 AND available_at IS NULL;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.suspend @id bigint, @attempt bigint, @until datetime2(7) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF @until IS NULL THROW 50003, 'Wake time is required', 1;
        IF NOT EXISTS(SELECT 1 FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0 AND available_at>SYSUTCDATETIME())
            THROW 50001, 'Job claim is no longer valid', 1;
        IF @until<=SYSUTCDATETIME() SELECT CAST(0 AS bit) AS suspended;
        ELSE BEGIN
            UPDATE resume.jobs SET leased=0,available_at=@until WHERE id=@id;
            SELECT CAST(1 AS bit) AS suspended;
        END;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.wait_for @id bigint, @attempt bigint, @child bigint AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF NOT EXISTS(SELECT 1 FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND attempt=@attempt AND leased=1 AND completed=0 AND available_at>SYSUTCDATETIME())
            THROW 50001, 'Job claim is no longer valid', 1;
        DECLARE @completed bit, @output nvarchar(max);
        SELECT @completed=completed,@output=output FROM resume.jobs WITH (READCOMMITTEDLOCK) WHERE id=@child AND parent_id=@id;
        IF @completed IS NULL THROW 50002, 'Job is not a child of this parent', 1;
        IF @completed=1 SELECT @output AS output;
        ELSE BEGIN
            UPDATE resume.jobs SET leased=0,available_at=NULL WHERE id=@id;
            SELECT CAST(NULL AS nvarchar(max)) AS output;
        END;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.resolve_step @id bigint, @key nvarchar(max), @output nvarchar(max) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        IF @output IS NULL OR ISJSON(@output,VALUE)<>1 THROW 50003, 'Pass a verified JSON output', 1;
        IF NOT EXISTS(SELECT 1 FROM resume.jobs WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND completed=0 AND (leased=0 OR available_at<=SYSUTCDATETIME()))
            THROW 50004, 'Job must be unfinished and not actively leased', 1;
        UPDATE resume.steps SET output=@output WHERE job_id=@id AND key_hash=HASHBYTES('SHA2_256',@key) AND once=1 AND output IS NULL;
        IF @@ROWCOUNT=0 THROW 50004, 'No unresolved step_once with this key', 1;
        UPDATE resume.jobs SET paused=1,leased=0 WHERE id=@id;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.requeue @id bigint, @delay_seconds float = 0 AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @ready datetime2=resume.after_delay(@delay_seconds), @paused bit, @leased bit;
        IF @ready IS NULL THROW 50003, 'Delay must be finite and nonnegative', 1;
        SELECT @paused=paused,@leased=leased FROM resume.jobs WITH (UPDLOCK,HOLDLOCK)
        WHERE id=@id AND completed=0 AND (leased=0 OR available_at<=SYSUTCDATETIME());
        IF @paused IS NULL THROW 50004, 'Job must be unfinished and not actively leased', 1;
        IF EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND output IS NULL) THROW 50004, 'Resolve unknown step outcomes before retrying', 1;
        IF @paused=0 AND @leased=0 THROW 50004, 'Job is already queued', 1;
        UPDATE resume.jobs SET paused=0,leased=0,available_at=@ready WHERE id=@id;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.begin_saga @id bigint, @attempt bigint AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        EXEC resume.begin_step @id,@attempt;
        IF EXISTS(SELECT 1 FROM resume.jobs WHERE id=@id AND saga_phase IS NULL)
        BEGIN
            IF EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id) THROW 50002, 'Saga must start before ordinary steps', 1;
            UPDATE resume.jobs SET saga_phase='forward' WHERE id=@id;
        END;
        SELECT * FROM resume.jobs WHERE id=@id;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.compensate @id bigint, @attempt bigint, @position int, @reason nvarchar(max) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        EXEC resume.begin_step @id,@attempt;
        IF @reason IS NULL OR ISJSON(@reason,VALUE)<>1 OR @position IS NULL OR @position<0
            OR NOT EXISTS(SELECT 1 FROM resume.jobs WHERE id=@id AND saga_phase='forward')
            OR EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND (output IS NULL OR position>=@position))
            THROW 50002, 'Cannot compensate: invalid state, unknown or omitted steps', 1;
        UPDATE resume.jobs SET saga_phase='compensating',saga_position=@position,saga_result=@reason WHERE id=@id;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE resume.end_saga @id bigint, @attempt bigint, @position int, @output nvarchar(max) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own = 1 BEGIN TRANSACTION;
    BEGIN TRY
        EXEC resume.begin_step @id,@attempt;
        DECLARE @phase varchar(16);
        SELECT @phase=saga_phase FROM resume.jobs WHERE id=@id;
        IF ISNULL(@phase,'') NOT IN ('forward','compensating') OR @position IS NULL OR @position<0 OR @output IS NULL OR ISJSON(@output,VALUE)<>1
            OR EXISTS(SELECT 1 FROM resume.steps WHERE job_id=@id AND (output IS NULL OR position>=@position))
            THROW 50002, 'Cannot finish saga: invalid state, unknown or omitted steps', 1;
        IF @phase='compensating' AND (SELECT COUNT_BIG(*) FROM resume.steps WHERE job_id=@id AND compensation IS NOT NULL)
            <> (SELECT COUNT_BIG(*) FROM resume.steps WHERE job_id=@id AND is_compensation=1 AND output IS NOT NULL)
            THROW 50002, 'Cannot finish saga: compensations remain', 1;
        UPDATE resume.jobs SET saga_phase=CASE WHEN @phase='forward' THEN 'completed' ELSE 'compensated' END,
            saga_position=@position,saga_result=CASE WHEN @phase='forward' THEN @output ELSE saga_result END WHERE id=@id;
        IF @own = 1 COMMIT;
    END TRY
    BEGIN CATCH
        IF @own = 1 AND XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
