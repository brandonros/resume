using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;

namespace Resume;

public static class Json
{
    public static JsonElement Null => JsonSerializer.SerializeToElement<object?>(null);
    public static JsonElement Value<T>(T value) => JsonSerializer.SerializeToElement(value);
    public static JsonElement Parse(string value) { using var doc = JsonDocument.Parse(value); return doc.RootElement.Clone(); }

    // Preserve jsonb-style equality at the API boundary: object order and numeric spelling
    // don't change submission identity. SQL-only callers must use this canonical form too.
    public static string Canonical(JsonElement value)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream)) Write(writer, value);
        return Encoding.UTF8.GetString(stream.ToArray());
    }
    private static void Write(Utf8JsonWriter writer, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                var properties = new SortedDictionary<string, JsonElement>(StringComparer.Ordinal);
                foreach (var property in value.EnumerateObject()) properties[property.Name] = property.Value;
                foreach (var (name, item) in properties) { writer.WritePropertyName(name); Write(writer, item); }
                writer.WriteEndObject(); break;
            case JsonValueKind.Array:
                writer.WriteStartArray(); foreach (var item in value.EnumerateArray()) Write(writer, item); writer.WriteEndArray(); break;
            case JsonValueKind.Number: writer.WriteRawValue(Number(value.GetRawText())); break;
            case JsonValueKind.Undefined: throw new ArgumentException("Undefined is not JSON; use Json.Null");
            default: value.WriteTo(writer); break;
        }
    }
    private static string Number(string raw)
    {
        var split = raw.Split(['e', 'E']);
        var exponent = split.Length == 2 ? BigInteger.Parse(split[1], CultureInfo.InvariantCulture) : BigInteger.Zero;
        var negative = split[0].StartsWith('-');
        var mantissa = split[0].TrimStart('-');
        var dot = mantissa.IndexOf('.');
        if (dot >= 0) exponent -= mantissa.Length - dot - 1;
        var digits = mantissa.Replace(".", "").TrimStart('0');
        if (digits.Length == 0) return "0";
        var trimmed = digits.TrimEnd('0'); exponent += digits.Length - trimmed.Length;
        string normalized;
        if (exponent >= 0 && exponent <= 10000) normalized = trimmed + new string('0', (int)exponent);
        else if (exponent < 0 && exponent >= -10000)
        {
            var point = trimmed.Length + (int)exponent;
            normalized = point > 0 ? trimmed.Insert(point, ".") : "0." + new string('0', -point) + trimmed;
        }
        else normalized = trimmed + "e" + exponent.ToString(CultureInfo.InvariantCulture);
        return (negative ? "-" : "") + normalized;
    }
}
