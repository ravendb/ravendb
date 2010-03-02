using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Json
{
    public class JsonDynamicObject : IEnumerable<JsonDynamicObject>
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

        public static bool operator ==(JsonDynamicObject dyn, string val)
        {
            if (ReferenceEquals(dyn, null))
                return val == null;
            return Equals(dyn.Value, val);
        }

        public static bool operator !=(JsonDynamicObject dyn, string val)
        {
            return !(dyn == val);
        }

        public static bool operator ==(JsonDynamicObject dyn, bool val)
        {
            return Equals(dyn.Value, val);
        }

        public static bool operator !=(JsonDynamicObject dyn, bool val)
        {
            return Equals(dyn.Value, val) == false;
        }

        public static bool operator ==(JsonDynamicObject dyn, int val)
        {
            return Equals(dyn.Value, val);
        }

        public static bool operator !=(JsonDynamicObject dyn, int val)
        {
            return Equals(dyn.Value, val) == false;
        }

        public static bool operator >(JsonDynamicObject dyn, int val)
        {
            return Convert.ToInt32(dyn.Value) > val;
        }

        public static bool operator <(JsonDynamicObject dyn, int val)
        {
            return Convert.ToInt32(dyn.Value) < val;
        }

        public static bool operator >=(JsonDynamicObject dyn, int val)
        {
            return Convert.ToInt32(dyn.Value) >= val;
        }

        public static bool operator <=(JsonDynamicObject dyn, int val)
        {
            return Convert.ToInt32(dyn.Value) <= val;
        }

        public bool Equals(JsonDynamicObject other)
        {
            return !ReferenceEquals(null, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof(JsonDynamicObject)) return false;
            return Equals(obj);
        }

        public override int GetHashCode()
        {
            if (Value == null)
                return 0;
            return Value.GetHashCode();
        }

        public JsonDynamicObject this[string key]
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

        public object Value
        {
            get
            {
                var value = json as JValue;
                if (value != null && value.Value != null)
                    return value.Value;
                return null;
            }
        }

        public IEnumerator<JsonDynamicObject> GetEnumerator()
        {
            var jArray = json as JArray;
            if(jArray == null)
                throw new InvalidOperationException("Can't iterate on an instance of " + json.GetType().Name);
            foreach (JToken item in jArray)
            {
                yield return new JsonDynamicObject(item);
            }
        }

        public override string ToString()
        {
            if(Value==null)
                return null;
            return Value.ToString();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}