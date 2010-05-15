using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;

namespace Raven.Database.Json
{
    public class JsonPatcher
    {
        private readonly JObject document;

        public JsonPatcher(JObject document)
        {
            this.document = document;
        }

		public JObject Apply(PatchRequest[] patch)
        {
            foreach (var patchCmd in patch)
            {
                Apply(patchCmd);
            }
            return document;
        }

        private void Apply(PatchRequest patchCmd)
        {
            if (patchCmd.Type == null)
                throw new InvalidOperationException("Patch property must have a type property");
            if (patchCmd.Name == null)
                throw new InvalidOperationException("Patch property must have a name property");
        	switch (patchCmd.Type.ToLowerInvariant())
            {
                case "set":
                    AddProperty(patchCmd, patchCmd.Name);
                    break;
                case "unset":
                    RemoveProperty(patchCmd, patchCmd.Name);
                    break;
                case "add":
                    AddValue(patchCmd, patchCmd.Name);
                    break;
                case "insert":
                    InsertValue(patchCmd, patchCmd.Name);
                    break;
                case "remove":
                    RemoveValue(patchCmd, patchCmd.Name);
                    break;
                case "modify":
                    ModifyValue(patchCmd, patchCmd.Name);
                    break;
                case "inc":
                    IncrementProperty(patchCmd, patchCmd.Name);
                    break;
                default:
					throw new ArgumentException("Cannot understand command: " + patchCmd.Type);
            }
        }

		private void ModifyValue(PatchRequest patchCmd, string propName)
        {
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
                throw new InvalidOperationException("Cannot modify value from  '" + propName + "' because it was not found");

            var nestedCommands = patchCmd.Nested;
            if (nestedCommands == null)
                throw new InvalidOperationException("Cannot understand modified value from  '" + propName +
                                                    "' because could not find nested array of commands");
            
            switch (property.Value.Type)
            {
                case JsonTokenType.Object:
                    foreach (var cmd in nestedCommands)
                    {
                        var nestedDoc = property.Value.Value<JObject>();
						new JsonPatcher(nestedDoc).Apply(cmd);
                    }
                    break;
                case JsonTokenType.Array:
					var position = patchCmd.Position;
					if (position == null)
                         throw new InvalidOperationException("Cannot modify value from  '" + propName +
                                                             "' because position element does not exists or not an integer");
                    var value = property.Value.Value<JArray>()[position];
					foreach (var cmd in nestedCommands)
					{
                    	new JsonPatcher(value.Value<JObject>()).Apply(cmd);
                    }
            		break;
                default:
                    throw new InvalidOperationException("Can't understand how to deal with: " + property.Value.Type);
            }
        }

        private void RemoveValue(PatchRequest patchCmd, string propName)
        {
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName, new JArray());
                document.Add(property);
            }
            var array = property.Value as JArray;
            if (array == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because it is not an array");
			var position = patchCmd.Position;
			if (position == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because position element does not exists or not an integer");
            if (position < 0 || position >= array.Count)
                throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
                                                   "' because position element is out of bound bounds");
            array.RemoveAt(position.Value);
        }

		private void InsertValue(PatchRequest patchCmd, string propName)
        {
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName, new JArray());
                document.Add(property);
            }
            var array = property.Value as JArray;
            if (array == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because it is not an array");
			var position = patchCmd.Position;
			if (position == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because position element does not exists or not an integer");
            if (position < 0 || position >= array.Count)
                throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
                                                   "' because position element is out of bound bounds");
            array.Insert(position.Value, patchCmd.Value);
        }

		private void AddValue(PatchRequest patchCmd, string propName)
        {
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName, new JArray());
                document.Add(property);
            }
            var array = property.Value as JArray;
            if (array == null)
                throw new InvalidOperationException("Cannot insert value to '" + propName + "' because it is not an array");

            array.Add(patchCmd.Value);
        }

		private void RemoveProperty(PatchRequest patchCmd, string propName)
        {
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property != null)
                property.Remove();
        }

        private void AddProperty(PatchRequest patchCmd, string propName)
        {
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName);
                document.Add(property);
            }
        	property.Value = patchCmd.Value;
        }


        private void IncrementProperty(PatchRequest patchCmd, string propName)
        {
            if(patchCmd.Value.Type != JsonTokenType.Integer)
                throw new InvalidOperationException("Cannot increment when value is not an integer");
            var property = document.Property(propName);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName);
                document.Add(property);
            }
            if (property.Value == null || property.Value.Type == JsonTokenType.Null)
                property.Value = patchCmd.Value;
            else
                property.Value = JToken.FromObject(property.Value.Value<int>() + patchCmd.Value.Value<int>());
        }
		private static void EnsurePreviousValueMatchCurrentValue(PatchRequest patchCmd, JProperty property)
        {
            var prevVal = patchCmd.PrevVal;
            if (prevVal == null)
                return;
            switch (prevVal.Type)
            {
                case JsonTokenType.Undefined:
                    if (property != null)
                        throw new ConcurrencyException();
                    break;
                default:
                    if(property == null)
                        throw new ConcurrencyException();
                    var equalityComparer = new JTokenEqualityComparer();
                    if (equalityComparer.Equals(property.Value, prevVal) == false)
                        throw new ConcurrencyException();
                    break;
            }
        }
    }
}