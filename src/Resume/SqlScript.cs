using System.Text.RegularExpressions;

namespace Resume;

public static class SqlScript
{
    // Installer for this repository's scripts, whose GO separators occupy their own lines.
    public static async Task ApplyAsync(Db db, string script, CancellationToken ct = default)
    {
        foreach (var batch in Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase))
            if (!string.IsNullOrWhiteSpace(batch)) await db.ExecAsync(batch, ct);
    }
}
