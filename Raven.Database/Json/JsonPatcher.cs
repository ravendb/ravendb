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
				Apply(patchCmd);
			}
			return document;
		}

		private void Apply(JObject patchCmd)
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
				case "modify":
					ModifyValue(patchCmd, patchCmd["name"].Value<string>());
					break;
				default:
					throw new ArgumentException("Cannot understand command: " + propName);
			}
		}

		private void ModifyValue(JObject patchCmd, string propName)
		{
			var property = document.Property(propName);
			if (property == null)
				throw new InvalidOperationException("Cannot modify value from  '" + propName + "' because it was not found");

			var val = patchCmd["value"];
			if (val == null || val.Type != JsonTokenType.Array)
				throw new InvalidOperationException("Cannot understand modified value from  '" + propName +
					"' because it was not found or not an array of commands");

			switch (property.Value.Type)
			{
				case JsonTokenType.Object:
					foreach (JToken cmd in val.Value<JArray>())
					{
						var nestedDoc = property.Value.Value<JObject>();
						new JsonPatcher(nestedDoc)
							.Apply(cmd.Value<JObject>());
					}
					break;
				case JsonTokenType.Array:
					var positionToken = patchCmd["position"];
					if (positionToken == null || positionToken.Type != JsonTokenType.Integer)
						throw new InvalidOperationException("Cannot modify value from  '" + propName +
							"' because position element does not exists or not an integer");
					var position = positionToken.Value<int>();
					var value = property.Value.Value<JArray>()[position];
					foreach (JToken cmd in val.Value<JArray>())
					{
						new JsonPatcher(value.Value<JObject>())
							.Apply(cmd.Value<JObject>());
					}
					break;
				default:
					throw new InvalidOperationException("Can't understand how to deal with: " + property.Value.Type);
			}
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
			if (positionToken == null || positionToken.Type != JsonTokenType.Integer)
				throw new InvalidOperationException("Cannot remove value from  '" + propName +
					"' because position element does not exists or not an integer");
			var position = positionToken.Value<int>();
			if (position < 0 || position >= array.Count)
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
				throw new InvalidOperationException("Cannot remove value from  '" + propName +
					"' because position element does not exists or not an integer");
			var position = positionToken.Value<int>();
			if (position < 0 || position >= array.Count)
				throw new IndexOutOfRangeException("Cannot remove value from  '" + propName +
					"' because position element is out of bound bounds");
			array.Insert(position, patchCmd["value"]);
		}

		private void AddValue(JObject patchCmd, string propName)
		{
			var property = document.Property(propName);
			if (property == null)
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