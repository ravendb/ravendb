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
            dynamic child = ConvertChild(token);
            var metadata = token["@metadata"];
            if (metadata != null)
            {
                var id = metadata["@id"];
                if(id != null)
                {
                    child.__document_id = id.Value<string>();
                }
            }
            return child;
        }

        private static object ConvertChild(IEnumerable<JToken> token)
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
                    expando.Add(property.Name, ConvertChild(property.Value));
                }
                return expando;
            }
            
            var jArray = token as JArray;
            if (jArray == null)
                throw new ArgumentException("Unknown token type: " + token);

            var array = new object[jArray.Count];
            var index = 0;
            foreach (JToken arrayItem in jArray)
            {
                array[index] = ConvertChild(arrayItem);
                index++;
            }
            return array;
        }
    }
}