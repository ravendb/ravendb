using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Database.Json;

namespace Raven.Client.Client
{
    public static class JsonExtensions
    {
        public static T Deserialize<T>(this JObject self, IContractResolver resolver)
        {
            var jsonSerializer = new JsonSerializer{ContractResolver =  resolver};
            jsonSerializer.Converters.Add(new JsonEnumConverter());
            return (T)jsonSerializer.Deserialize(new JTokenReader(self), typeof(T));
        }

        public static object Deserialize(this JObject self, Type type, IContractResolver resolver)
        {
            var jsonSerializer = new JsonSerializer { ContractResolver = resolver };
            jsonSerializer.Converters.Add(new JsonEnumConverter());
            return jsonSerializer.Deserialize(new JTokenReader(self), type);
        }
    }
}