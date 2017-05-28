using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Extensions
{
    public static class JsonSerializationExtensions
    {
        public static DynamicJsonValue ToJson<TValue>(this Dictionary<string, TValue> dic)
            where TValue : struct
        {
            var jsonMap = new DynamicJsonValue();

            foreach (var kvp in dic)
            {
                jsonMap[kvp.Key] = kvp.Value;
            }

            return jsonMap;
        }

        public static DynamicJsonValue ToJsonWithConvertible<TValue>(this Dictionary<string, TValue> dic)
            where TValue : IDynamicJsonValueConvertible
        {
            var jsonMap = new DynamicJsonValue();

            foreach (var kvp in dic)
            {
                jsonMap[kvp.Key] = kvp.Value.ToJson();
            }

            return jsonMap;
        }
    }
}
