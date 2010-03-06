using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Json
{
    public class JsonPatcher
    {
        private readonly JObject document;

        public JsonPatcher(JObject document)
        {
            this.document = document;
        }

        public JObject Apply(JArray patch)
        {
            foreach (JObject patchCmd in patch)
            {
                if (patchCmd["type"] == null)
                    throw new InvalidOperationException("Patch property must have a type property");
                if (patchCmd["name"] == null)
                    throw new InvalidOperationException("Patch property must have a name property");
                var propName = patchCmd["type"].Value<string>();
                switch (propName)
                {
                    case "set":
                        AddProperty(patchCmd, patchCmd["name"].Value<string>());
                        break;
                    case "unset":
                        RemoveProperty(patchCmd["name"].Value<string>());
                        break;
                    case "add":
                        AddValue(patchCmd, patchCmd["name"].Value<string>());
                        break;
                    case "insert":
                        InsertValue(patchCmd, patchCmd["name"].Value<string>());
                        break;
                    case "remove":
                        RemoveValue(patchCmd, patchCmd["name"].Value<string>());
                        break;
                    default:
                        throw new ArgumentException("Cannot understand command: " + propName);
                }
            }
            return document;
        }

        private void RemoveValue(JObject patchCmd, string propName)
        {
            var property = document.Property(propName);
            if (property == null)
            {
                property = new JProperty(propName, new JArray());
                document.Add(property);
            }
            var array = property.Value as JArray;
            if (array == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because it is not an array");
            var positionToken = patchCmd["position"];
            if(positionToken == null || positionToken.Type != JsonTokenType.Integer)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because position element does not exists or not an integer");
            var position = positionToken.Value<int>();
            if(position< 0 || position >= array.Count)
                throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
                                                   "' because position element is out of bound bounds");
            array.RemoveAt(position);
        }

        private void InsertValue(JObject patchCmd, string propName)
        {
            var property = document.Property(propName);
            if (property == null)
            {
                property = new JProperty(propName, new JArray());
                document.Add(property);
            }
            var array = property.Value as JArray;
            if (array == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because it is not an array");
            var positionToken = patchCmd["position"];
            if (positionToken == null || positionToken.Type != JsonTokenType.Integer)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because position element does not exists or not an integer");
            var position = positionToken.Value<int>();
            if (position < 0 || position >= array.Count)
                throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
                                                   "' because position element is out of bound bounds");
            array.Insert(position, patchCmd["value"]);
        }

        private void AddValue(JObject patchCmd, string propName)
        {
            var property = document.Property(propName);
            if(property == null)
            {
                property = new JProperty(propName, new JArray());
                document.Add(property);
            }
            var array = property.Value as JArray;
            if (array == null)
                throw new InvalidOperationException("Cannot insert value to '" + propName + "' because it is not an array");

            array.Add(patchCmd["value"]);
        }

        private void RemoveProperty(string propName)
        {
            var property = document.Property(propName);
            if (property != null)
                property.Remove();
        }

        private void AddProperty(JObject patchCmd, string propName)
        {
            var property = document.Property(propName);
            if (property == null)
            {
                property = new JProperty(propName);
                document.Add(property);
            }
            property.Value = patchCmd["value"];
        }
    }
}