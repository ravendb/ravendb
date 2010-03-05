using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Json
{
    public static class JsonToExpando
    {
        public static object Convert(JToken token)
        {
            var jValue = token as JValue;
            if (jValue != null)
            {
                return jValue.Value;
            }
            var jObject = token as JObject;
            if (jObject != null)
            {
                var expando = new ExpandoObject() as IDictionary<string, object>;
                foreach (var property in (from childToken in token where childToken is JProperty select childToken as JProperty))
                {
                    expando.Add(property.Name, Convert(property.Value));
                }
                return expando;
            }
            var jArray = token as JArray;
            if (jArray != null)
            {
                var array = new object[jArray.Count];
                var index = 0;
                foreach (JToken arrayItem in jArray)
                {
                    array[index] = Convert(arrayItem);
                    index++;
                }
                return array;
            }
            throw new ArgumentException("Unknown token type: " + token);
        }
    }
}