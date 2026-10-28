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
            if (double.IsNaN(value) || double.IsInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }

        private static string Format(decimal value)
        {
            string text = value.ToString("G", CultureInfo.InvariantCulture);
            return text.IndexOf('.') != -1 ? text : text + ".0";
        }

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
                writer.WriteRawValue(Format(value), skipInputValidation: true);
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
                writer.WriteRawValue(Format((double)value), skipInputValidation: true);
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
                writer.WriteRawValue(Format(value), skipInputValidation: true);
            }
        }
    }
}
