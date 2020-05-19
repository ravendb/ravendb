using System;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Json.Serialization.JsonNet.Internal;

namespace Raven.Server.Json.Converters
{
    internal class CounterOperationConverter : RavenTypeJsonConverter<CounterOperation>
    {
        public static CounterOperationConverter Instance = new CounterOperationConverter();

        private CounterOperationConverter()
        {
        }

        protected override void WriteJson(BlittableJsonWriter writer, CounterOperation value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartObject();

            writer.WritePropertyName(nameof(CounterOperation.CounterName));
            if (value.CounterName != null)
                writer.WriteValue(value.CounterName);
            else
                writer.WriteNull();

            writer.WritePropertyName(nameof(CounterOperation.Delta));
            writer.WriteValue(value.Delta);

            writer.WritePropertyName(nameof(CounterOperation.Type));
            writer.WriteValue(value.Type);

            writer.WritePropertyName(nameof(CounterOperation.DocumentId));
            if (value.DocumentId != null)
                writer.WriteValue(value.DocumentId);
            else
                writer.WriteNull();

            writer.WritePropertyName(nameof(CounterOperation.ChangeVector));
            if (value.ChangeVector != null)
                writer.WriteValue(value.ChangeVector);
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        internal override CounterOperation ReadJson(BlittableJsonReader blittableReader)
        {
            var result = new CounterOperation();

            do
            {
                blittableReader.Read();
                if (blittableReader.TokenType == JsonToken.EndObject)
                    return result;
                if (blittableReader.TokenType != JsonToken.PropertyName)
                    throw new InvalidOperationException("Expected PropertyName, Got " + blittableReader.TokenType);

                var property = (string)blittableReader.Value;
                switch (property)
                {
                    case nameof(CounterOperation.CounterName):
                        result.CounterName = blittableReader.ReadAsString();
                        break;
                    case nameof(CounterOperation.Delta):
                        blittableReader.Read();
                        result.Delta = (long)blittableReader.Value;
                        break;
                    case nameof(CounterOperation.Type):
                        result.Type = Enum.Parse<CounterOperationType>(blittableReader.ReadAsString());
                        break;
                    case nameof(CounterOperation.DocumentId):
                        result.DocumentId = blittableReader.ReadAsString();
                        break;
                    case nameof(CounterOperation.ChangeVector):
                        result.ChangeVector = blittableReader.ReadAsString();
                        break;
                }
            } while (true);
        }
    }
}
