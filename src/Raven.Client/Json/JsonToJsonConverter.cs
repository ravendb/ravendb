using System;
using Newtonsoft.Json;

namespace Raven.Client.Json
{
    public class JsonToJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
            /*if (value is RavenJToken)
                ((RavenJToken)value).WriteTo(writer);
            else if(value is DynamicNullObject)
                writer.WriteNull();
            else
                ((IDynamicJsonObject)value).WriteTo(writer);*/
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();

            // NOTE: THIS DOESN'T SUPPORT READING OF DynamicJsonObject !!!

            /*var o = RavenJToken.Load(reader);
            return (o.Type == JTokenType.Null || o.Type == JTokenType.Undefined) ? null : o;*/
        }

        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();

            /*return objectType == typeof (RavenJToken) ||
                   objectType == typeof (DynamicJsonObject) ||
                   objectType == typeof (DynamicNullObject) ||
                   objectType.GetTypeInfo().IsSubclassOf(typeof (RavenJToken)) ||
                   objectType.GetTypeInfo().IsSubclassOf(typeof (DynamicJsonObject));*/
        }
    }
}
