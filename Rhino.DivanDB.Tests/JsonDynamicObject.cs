using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Tests
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

        protected override object Value
        {
            get
            {
                var value = json as JValue;
                if (value != null)
                    return value.Value;
                return null;
            }
        }
    }
}