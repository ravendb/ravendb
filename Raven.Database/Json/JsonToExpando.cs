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
                if (id != null)
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
                var value = jValue.Value;
                var str = value as string;
                if (str != null)
                {
                    int iResult;
                    if (int.TryParse(str, out iResult))
                        return iResult;
                    double dblResult;
                    if (double.TryParse(str, out dblResult))
                        return dblResult;
                    bool bResult;
                    if (bool.TryParse(str, out bResult))
                        return bResult;
                }
                return value;
            }
            var jObject = token as JObject;
            if (jObject != null)
            {
                var expando = new ExpandoObject() as IDictionary<string, object>;
                foreach (var property in token.OfType<JProperty>())
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