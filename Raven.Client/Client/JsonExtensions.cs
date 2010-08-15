using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Client.Document;
using Raven.Database.Json;

namespace Raven.Client.Client
{
    public static class JsonExtensions
    {
        public static T Deserialize<T>(this JObject self, DocumentConvention convention)
        {
			return (T)convention.CreateSerializer().Deserialize(new JTokenReader(self), typeof(T));
        }

        public static object Deserialize(this JObject self, Type type, DocumentConvention convention)
        {
			return convention.CreateSerializer().Deserialize(new JTokenReader(self), type);
        }
    }
}