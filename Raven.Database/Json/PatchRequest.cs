using System;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Database.Json
{
	public enum PatchCommandType
	{
		Set,
		Unset,
		Add,
		Insert,
		Remove,
		Modify,
		Inc,
		Copy,
		Move
	}

	public class PatchRequest
	{
		public PatchCommandType Type { get; set; }
		public JToken PrevVal { get; set; }
		public JToken Value { get; set; }
		public PatchRequest[] Nested { get; set; }
		public string Name { get; set; }
		public int? Position { get; set; }

		public JObject ToJson()
		{
			var jObject = new JObject(
				new JProperty("Type", new JValue(Type.ToString())),
				new JProperty("Value", Value),
				new JProperty("Name", new JValue(Name)),
				new JProperty("Position", Position == null ? null : new JValue(Position.Value)),
				new JProperty("Nested", Nested == null ? null : new JArray(Nested.Select(x => x.ToJson())))
				);
			if (PrevVal != null)
				jObject.Add(new JProperty("PrevVal", PrevVal));
			return jObject;
		}

		public static PatchRequest FromJson(JObject patchRequestJson)
		{
			PatchRequest[] nested = null;
			var nestedJson = patchRequestJson.Value<JValue>("Nested");
			if (nestedJson != null && nestedJson.Value != null)
				nested = nestedJson.Value<JArray>().Cast<JObject>().Select(FromJson).ToArray();

			return new PatchRequest
			{
				Type = (PatchCommandType)Enum.Parse(typeof(PatchCommandType), patchRequestJson.Value<string>("Type")),
				Name = patchRequestJson.Value<string>("Name"),
				Nested = nested,
				Position = (int?)patchRequestJson.Value<object>("Position"),
				PrevVal = patchRequestJson.Property("PrevVal") == null ? null : patchRequestJson.Property("PrevVal").Value,
				Value = patchRequestJson.Property("Value") == null ? null : patchRequestJson.Property("Value").Value,
			};
		}
	}
}