using System.Data;
using Microsoft.Data.SqlClient;

namespace Resume;

/// <summary>A connection with an optional caller-owned transaction. Never share concurrently.</summary>
public sealed class Db(SqlConnection connection, SqlTransaction? transaction = null)
{
    public SqlConnection Connection { get; } = connection;
    public SqlTransaction? Transaction { get; } = transaction;
    public Task<List<Row>> QueryAsync(string sql, params (string, object?)[] args) => QueryAsync(sql, CancellationToken.None, args);
    public Task<List<Row>> QueryAsync(string sql, CancellationToken ct, params (string, object?)[] args) => Read(sql, CommandType.Text, ct, args);
    public Task<List<Row>> ProcAsync(string name, CancellationToken ct, params (string, object?)[] args) => Read("resume." + name, CommandType.StoredProcedure, ct, args);
    public Task<int> ExecAsync(string sql, params (string, object?)[] args) => ExecAsync(sql, CancellationToken.None, args);
    public async Task<int> ExecAsync(string sql, CancellationToken ct, params (string, object?)[] args)
    {
        using var command = Command(sql, CommandType.Text, args);
        return await command.ExecuteNonQueryAsync(ct);
    }
    private SqlCommand Command(string sql, CommandType type, (string Name, object? Value)[] args)
    {
        var command = new SqlCommand(sql, Connection, Transaction) { CommandType = type, CommandTimeout = 60 };
        foreach (var (name, value) in args)
        {
            var parameter = value switch
            {
                long n => new SqlParameter("@" + name, SqlDbType.BigInt) { Value = n },
                int n => new SqlParameter("@" + name, SqlDbType.Int) { Value = n },
                bool b => new SqlParameter("@" + name, SqlDbType.Bit) { Value = b },
                double d when double.IsFinite(d) => new SqlParameter("@" + name, SqlDbType.Float) { Value = d },
                double => throw new ArgumentOutOfRangeException(name, "Number must be finite"),
                DateTimeOffset dt => new SqlParameter("@" + name, SqlDbType.DateTime2) { Value = dt.UtcDateTime },
                DateTime dt => new SqlParameter("@" + name, SqlDbType.DateTime2) { Value = dt },
                null => new SqlParameter("@" + name, SqlDbType.NVarChar, -1) { Value = DBNull.Value },
                string text => new SqlParameter("@" + name, SqlDbType.NVarChar, -1) { Value = text },
                _ => throw new ArgumentException($"Unsupported SQL parameter {name}")
            };
            command.Parameters.Add(parameter);
        }
        return command;
    }
    private async Task<List<Row>> Read(string sql, CommandType type, CancellationToken ct, (string, object?)[] args)
    {
        using var command = Command(sql, type, args);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<Row>();
        // Drain every result so an error at a procedure's COMMIT cannot be mistaken for success.
        do
        {
            while (await reader.ReadAsync(ct))
            {
                var row = new Row();
                for (var i = 0; i < reader.FieldCount; i++) row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }
        } while (await reader.NextResultAsync(ct));
        return rows;
    }
}

public sealed class Row : Dictionary<string, object?>
{
    public Row() : base(StringComparer.OrdinalIgnoreCase) { }
    public long Long(string name) => Convert.ToInt64(this[name]);
    public int Int(string name) => Convert.ToInt32(this[name]);
    public bool Bool(string name) => Convert.ToBoolean(this[name]);
    public string Text(string name) => (string)(this[name] ?? throw new InvalidOperationException($"{name} is SQL NULL"));
    public System.Text.Json.JsonElement Json(string name) => Resume.Json.Parse(Text(name));
}
