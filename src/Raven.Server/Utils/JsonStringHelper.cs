using System;
using System.Buffers;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Server.Utils;

public sealed class JsonStringHelper
{
    private static readonly JsonDocumentOptions ReadOptions = new()
    {
        CommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    private static readonly JsonWriterOptions WriteOptions = new()
    {
        Indented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private static readonly JsonSerializerOptions SerializerOptions = CreateSerializerOptions(indented: false);

    private static readonly JsonSerializerOptions IndentedSerializerOptions = CreateSerializerOptions(indented: true);

    private static JsonSerializerOptions CreateSerializerOptions(bool indented)
    {
        return new JsonSerializerOptions
        {
            WriteIndented = indented,
            IncludeFields = true,
            PropertyNameCaseInsensitive = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            NumberHandling = JsonNumberHandling.AllowReadingFromString,
            ReadCommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true,
            Converters = { ScalarToStringConverter.Instance }
        };
    }

    private static readonly JsonSerializerOptions DeserializerOptions = new(SerializerOptions)
    {
        Converters = { new JsonStringEnumConverter(), LenientDateTimeConverter.Instance }
    };

    private sealed class LenientDateTimeConverter : JsonConverter<DateTime>
    {
        public static readonly LenientDateTimeConverter Instance = new();

        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.String)
                return reader.GetDateTime();

            if (reader.TryGetDateTime(out DateTime value))
                return value;

            return DateTime.Parse(reader.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }

    private sealed class ScalarToStringConverter : JsonConverter<string>
    {
        public static readonly ScalarToStringConverter Instance = new();

        public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.Number:
                case JsonTokenType.True:
                case JsonTokenType.False:
                    return Encoding.UTF8.GetString(reader.HasValueSequence ? reader.ValueSequence.ToArray() : reader.ValueSpan);
                default:
                    return reader.GetString();
            }
        }

        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }

    public static string Serialize(object value, bool indented = false)
    {
        if (value == null)
            return "null";

        return JsonSerializer.Serialize(value, value.GetType(), indented ? IndentedSerializerOptions : SerializerOptions);
    }

    public static T Deserialize<T>(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return default;

        return JsonSerializer.Deserialize<T>(json, DeserializerOptions);
    }

    public static string Indent(string json)
    {
        using (JsonDocument document = JsonDocument.Parse(json, ReadOptions))
        using (var stream = new MemoryStream())
        {
            using (var writer = new Utf8JsonWriter(stream, WriteOptions))
                document.RootElement.WriteTo(writer);

            return Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
        }
    }
}
