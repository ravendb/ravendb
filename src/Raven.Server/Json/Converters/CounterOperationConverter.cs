using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.Counters;

namespace Raven.Server.Json.Converters
{
    internal sealed class CounterOperationConverter : JsonConverter<CounterOperation>
    {
        public static CounterOperationConverter Instance = new CounterOperationConverter();

        private CounterOperationConverter()
        {
        }

        public override void Write(Utf8JsonWriter writer, CounterOperation value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString(nameof(CounterOperation.CounterName), value.CounterName);
            writer.WriteNumber(nameof(CounterOperation.Delta), value.Delta);
            writer.WriteNumber(nameof(CounterOperation.Type), (int)value.Type);
            writer.WriteString(nameof(CounterOperation.DocumentId), value.DocumentId);
            writer.WriteString(nameof(CounterOperation.ChangeVector), value.ChangeVector);
            writer.WriteEndObject();
        }

        public override CounterOperation Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new InvalidOperationException("Expected StartObject, Got " + reader.TokenType);

            var result = new CounterOperation();

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return result;
                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw new InvalidOperationException("Expected PropertyName, Got " + reader.TokenType);

                string property = reader.GetString();
                reader.Read();

                switch (property)
                {
                    case nameof(CounterOperation.CounterName):
                        result.CounterName = reader.GetString();
                        break;
                    case nameof(CounterOperation.Delta):
                        result.Delta = reader.GetInt64();
                        break;
                    case nameof(CounterOperation.Type):
                        result.Type = reader.TokenType == JsonTokenType.Number
                            ? (CounterOperationType)reader.GetInt32()
                            : Enum.Parse<CounterOperationType>(reader.GetString());
                        break;
                    case nameof(CounterOperation.DocumentId):
                        result.DocumentId = reader.GetString();
                        break;
                    case nameof(CounterOperation.ChangeVector):
                        result.ChangeVector = reader.GetString();
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }

            throw new InvalidOperationException("Expected EndObject");
        }
    }
}
