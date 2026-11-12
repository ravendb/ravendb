using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal static class FloatingPointConverters
    {
        public static readonly JsonConverter<double> Double = new DoubleConverter();

        public static readonly JsonConverter<float> Single = new SingleConverter();

        public static readonly JsonConverter<decimal> Decimal = new DecimalConverter();

        private static string Format(double value)
        {
            string text = value.ToString("R", CultureInfo.InvariantCulture);
            if (text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }

        private static string Format(decimal value)
        {
            string text = value.ToString("G", CultureInfo.InvariantCulture);
            return text.IndexOf('.') != -1 ? text : text + ".0";
        }

        private static void Write(Utf8JsonWriter writer, double value)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
            {
                NamedFloatingPointLiterals.Write(writer, value);
                return;
            }

#if NET8_0_OR_GREATER
            Span<byte> buffer = stackalloc byte[MaxNumberLength];
            if (value.TryFormat(buffer.Slice(0, MaxNumberLength - 2), out int written, "R", CultureInfo.InvariantCulture))
            {
                WriteNumber(writer, buffer, written);
                return;
            }
#endif

            writer.WriteRawValue(Format(value), skipInputValidation: true);
        }

        private static void Write(Utf8JsonWriter writer, decimal value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buffer = stackalloc byte[MaxNumberLength];
            if (value.TryFormat(buffer.Slice(0, MaxNumberLength - 2), out int written, "G", CultureInfo.InvariantCulture))
            {
                WriteNumber(writer, buffer, written);
                return;
            }
#endif

            writer.WriteRawValue(Format(value), skipInputValidation: true);
        }

#if NET8_0_OR_GREATER
        private const int MaxNumberLength = 64;

        private static void WriteNumber(Utf8JsonWriter writer, Span<byte> buffer, int written)
        {
            if (buffer.Slice(0, written).IndexOfAny((byte)'.', (byte)'E', (byte)'e') == -1)
            {
                buffer[written++] = (byte)'.';
                buffer[written++] = (byte)'0';
            }

            writer.WriteRawValue(buffer.Slice(0, written), skipInputValidation: true);
        }
#endif

        private sealed class DoubleConverter : JsonConverter<double>
        {
            public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.String)
                    return double.Parse(reader.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture);

                return reader.GetDouble();
            }

            public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
            {
                FloatingPointConverters.Write(writer, value);
            }
        }

        private sealed class SingleConverter : JsonConverter<float>
        {
            public override float Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.String)
                    return float.Parse(reader.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture);

                return reader.GetSingle();
            }

            public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options)
            {
                FloatingPointConverters.Write(writer, (double)value);
            }
        }

        private sealed class DecimalConverter : JsonConverter<decimal>
        {
            public override decimal Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.String)
                    return decimal.Parse(reader.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture);

                return reader.GetDecimal();
            }

            public override void Write(Utf8JsonWriter writer, decimal value, JsonSerializerOptions options)
            {
                FloatingPointConverters.Write(writer, value);
            }
        }
    }
}
