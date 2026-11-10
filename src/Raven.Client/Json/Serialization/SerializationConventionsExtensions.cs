using System.IO;
using System.Text;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Json.Serialization
{
    internal static class SerializationConventionsExtensions
    {
        private const string ValuePropertyName = "Value";

        public static string SerializeToJson(this ISerializationConventions serialization, object value, JsonOperationContext context)
        {
            using (IJsonWriter writer = serialization.CreateWriter(context))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(ValuePropertyName);
                serialization.CreateSerializer().Serialize(writer, value);
                writer.WriteEndObject();
                writer.FinalizeDocument();

                using (BlittableJsonReaderObject json = writer.CreateReader())
                using (MemoryStream stream = RecyclableMemoryStreamFactory.GetRecyclableStream())
                {
                    var property = new BlittableJsonReaderObject.PropertyDetails();
                    json.GetPropertyByIndex(0, ref property);

                    using (var textWriter = new BlittableJsonTextWriter(context, stream))
                        textWriter.WriteValue(property.Token & BlittableJsonReaderBase.TypesMask, property.Value);

                    return Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
                }
            }
        }
    }
}
