using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Json
{
    public class JsonDynamicObject : DynamicObject
    {
        private readonly JToken json;

        public JsonDynamicObject(JToken json)
        {
            this.json = json;
        }

        public JsonDynamicObject(string jsonString)
        {
            var serializer = new JsonSerializer();
            using (var reader = new JsonTextReader(new StringReader(jsonString)))
                json = (JToken) serializer.Deserialize(reader);
        }

        public override DynamicObject this[string key]
        {
            get
            {
                var obj = json as JObject;
                if(obj == null)
                    return null;
                var property = obj.Property(key);
                if(property==null)
                    return null;
                return new JsonDynamicObject(property.Value);
            }
        }

        public override object Value
        {
            get
            {
                var value = json as JValue;
                if (value != null && value.Value != null)
                    return value.Value;
                return null;
            }
        }

        public override string ToString()
        {
            if(Value==null)
                return null;
            return Value.ToString();
        }
    }
}