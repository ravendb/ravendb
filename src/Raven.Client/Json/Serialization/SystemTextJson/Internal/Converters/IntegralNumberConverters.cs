using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal static class IntegralNumberConverters
    {
        public static readonly JsonConverter[] All =
        [
            new Int32Converter(),
            new Int64Converter(),
            new Int16Converter(),
            new ByteConverter(),
            new SByteConverter(),
            new UInt32Converter(),
            new UInt64Converter(),
            new UInt16Converter()
        ];

        private static decimal ReadTruncated(ref Utf8JsonReader reader)
        {
            if (reader.TokenType == JsonTokenType.String)
                return decimal.Truncate(decimal.Parse(reader.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture));

            if (reader.TryGetDecimal(out decimal value))
                return decimal.Truncate(value);

            return (decimal)Math.Truncate(reader.GetDouble());
        }

        private sealed class Int32Converter : JsonConverter<int>
        {
            public override int Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out int value) ? value : (int)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, int value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class Int64Converter : JsonConverter<long>
        {
            public override long Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long value) ? value : (long)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, long value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class Int16Converter : JsonConverter<short>
        {
            public override short Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetInt16(out short value) ? value : (short)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, short value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class ByteConverter : JsonConverter<byte>
        {
            public override byte Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetByte(out byte value) ? value : (byte)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, byte value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class SByteConverter : JsonConverter<sbyte>
        {
            public override sbyte Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetSByte(out sbyte value) ? value : (sbyte)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, sbyte value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class UInt32Converter : JsonConverter<uint>
        {
            public override uint Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetUInt32(out uint value) ? value : (uint)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, uint value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class UInt64Converter : JsonConverter<ulong>
        {
            public override ulong Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetUInt64(out ulong value) ? value : (ulong)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, ulong value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }

        private sealed class UInt16Converter : JsonConverter<ushort>
        {
            public override ushort Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                reader.TokenType == JsonTokenType.Number && reader.TryGetUInt16(out ushort value) ? value : (ushort)ReadTruncated(ref reader);

            public override void Write(Utf8JsonWriter writer, ushort value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
        }
    }
}
