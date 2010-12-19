using System;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Exceptions;
using Raven.Http.Exceptions;
using System.Linq;
using Raven.Abstractions.Json;

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
            if (patchCmd.Name == null)
                throw new InvalidOperationException("Patch property must have a name property");
            foreach (var token in document.SelectTokenWithRavenSyntax(patchCmd.Name))
            {
                JProperty property = null;
                if (token != null)
                    property = token.Parent as JProperty;
                switch (patchCmd.Type)
                {
                    case PatchCommandType.Set:
                        AddProperty(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Unset:
                        RemoveProperty(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Add:
                        AddValue(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Insert:
                        InsertValue(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Remove:
                        RemoveValue(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Modify:
                        ModifyValue(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Inc:
                        IncrementProperty(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Copy:
                        CopyProperty(patchCmd, patchCmd.Name, property);
                        break;
                    case PatchCommandType.Rename:
                        RenameProperty(patchCmd, patchCmd.Name, property);
                        break;
                    default:
                        throw new ArgumentException("Cannot understand command: " + patchCmd.Type);
                }
            }
        }

		private void RenameProperty(PatchRequest patchCmd, string propName, JProperty property)
		{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			if (property == null)
				throw new InvalidOperationException("Cannot copy value from  '" + propName + "' because it was not found");

			document[patchCmd.Value.Value<string>()] = property.Value;
			document.Remove(propName);
		}

		private void CopyProperty(PatchRequest patchCmd, string propName, JProperty property)
    	{
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
			if (property == null)
				throw new InvalidOperationException("Cannot copy value from  '" + propName + "' because it was not found");

			document[patchCmd.Value.Value<string>()] = property.Value;
    	}

    	private void ModifyValue(PatchRequest patchCmd, string propName, JProperty property)
        {
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
                throw new InvalidOperationException("Cannot modify value from  '" + propName + "' because it was not found");

            var nestedCommands = patchCmd.Nested;
            if (nestedCommands == null)
                throw new InvalidOperationException("Cannot understand modified value from  '" + propName +
                                                    "' because could not find nested array of commands");
            
            switch (property.Value.Type)
            {
                case JTokenType.Object:
                    foreach (var cmd in nestedCommands)
                    {
                        var nestedDoc = property.Value.Value<JObject>();
						new JsonPatcher(nestedDoc).Apply(cmd);
                    }
                    break;
                case JTokenType.Array:
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

        private void RemoveValue(PatchRequest patchCmd, string propName, JProperty property)
        {
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
            var value = patchCmd.Value;
			if (position == null && value == null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because position element does not exists or not an integer and no value was present");
            if (position != null && value != null)
                throw new InvalidOperationException("Cannot remove value from  '" + propName + "' because both a position and a value are set");
            if (position < 0 || position >= array.Count)
                throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
                                                   "' because position element is out of bound bounds");

            if (value != null)
            {
                var equalityComparer = new JTokenEqualityComparer();
                var singleOrDefault = array.FirstOrDefault(x => equalityComparer.Equals(x, value));
                if (singleOrDefault == null)
                    return;
                array.Remove(singleOrDefault);
                return;
            }
            array.RemoveAt(position.Value);
           
        }

		private void InsertValue(PatchRequest patchCmd, string propName, JProperty property)
        {
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

		private void AddValue(PatchRequest patchCmd, string propName, JProperty property)
        {
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

		private void RemoveProperty(PatchRequest patchCmd, string propName, JProperty property)
        {
			EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property != null)
                property.Remove();
        }

        private void AddProperty(PatchRequest patchCmd, string propName, JProperty property)
        {
        	EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName);
                document.Add(property);
            }
        	property.Value = patchCmd.Value;
        }


        private void IncrementProperty(PatchRequest patchCmd, string propName, JProperty property)
        {
            if(patchCmd.Value.Type != JTokenType.Integer)
                throw new InvalidOperationException("Cannot increment when value is not an integer");
        	EnsurePreviousValueMatchCurrentValue(patchCmd, property);
            if (property == null)
            {
                property = new JProperty(propName);
                document.Add(property);
            }
            if (property.Value == null || property.Value.Type == JTokenType.Null)
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
                case JTokenType.Undefined:
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
