using System;
using System.IO;
using System.Text;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Json.Serialization
{
    internal static class SerializationConventionsExtensions
    {
        internal const string ValuePropertyName = "Value";

        public static string SerializeToJson(this ISerializationConventions serialization, object value, JsonOperationContext context)
        {
            using (IJsonWriter writer = serialization.CreateWriter(context))
            {
                using (BlittableJsonReaderObject json = serialization.SerializeAsValueProperty(writer, value))
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

        public static BlittableJsonReaderObject SerializeAsValueProperty(this ISerializationConventions serialization, IJsonWriter writer, object value, Type declaredType = null)
        {
            IJsonSerializer serializer = serialization.CreateSerializer();

            writer.WriteStartObject();
            writer.WritePropertyName(ValuePropertyName);

            if (declaredType != null)
                serializer.Serialize(writer, value, declaredType);
            else
                serializer.Serialize(writer, value);

            writer.WriteEndObject();
            writer.FinalizeDocument();

            return writer.CreateReader();
        }
    }
}
