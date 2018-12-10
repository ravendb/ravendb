using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Extensions
{
    internal static class JsonSerializationExtensions
    {
        public static DynamicJsonValue ToJson<TValue>(this Dictionary<string, TValue> dic)     
        {
            var jsonMap = new DynamicJsonValue();
            if (dic == null) //precaution, prevent NRE
                return null;

            foreach (var kvp in dic)
            {
                jsonMap[kvp.Key] = kvp.Value;
            }

            return jsonMap;
        }


        public static DynamicJsonValue ToJson<TValue>(this Dictionary<StringSegment, TValue> dic)
        {
            var jsonMap = new DynamicJsonValue();
            if (dic == null) //precaution, prevent NRE
                return null;

            foreach (var kvp in dic)
            {
                jsonMap[kvp.Key.ToString()] = kvp.Value;
            }

            return jsonMap;
        }

        public static DynamicJsonValue ToJsonWithConvertible<TValue>(this Dictionary<string, TValue> dic)
            where TValue : IDynamicJsonValueConvertible
        {
            var jsonMap = new DynamicJsonValue();
            if (dic == null) //precaution, prevent NRE
                return null;

            foreach (var kvp in dic)
            {
                jsonMap[kvp.Key] = kvp.Value.ToJson();
            }

            return jsonMap;
        }
    }
}
