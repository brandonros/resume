using System.Data;
using Microsoft.Data.SqlClient;

namespace Resume;

public static class Schema
{
    /// <summary>The schema version this library requires; resume.schema_version records the installed one.</summary>
    public const int Version = 2;

    public static string Script()
    {
        using var stream = typeof(Schema).Assembly.GetManifestResourceStream("Resume.schema.sql")
            ?? throw new InvalidOperationException("Embedded schema is missing");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <summary>Installs or upgrades the resume schema. Safe to rerun and to run from several hosts at once.</summary>
    public static async Task InstallAsync(SqlConnection connection, CancellationToken ct = default)
    {
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var db = new Db(connection, tx);
        await db.ExecAsync("""
            DECLARE @result int;
            EXEC @result = sp_getapplock @Resource=N'resume.install', @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=60000;
            IF @result < 0 THROW 50007, 'Could not acquire the resume install lock', 1;
            """, ct);
        await SqlScript.ApplyAsync(db, Script(), ct);
        await tx.CommitAsync(ct);
    }
}
