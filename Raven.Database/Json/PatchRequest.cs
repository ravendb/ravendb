using System;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Database.Json
{
	public class PatchRequest
	{
		public string Type{ get; set;}
		public JToken PrevVal { get; set; }
		public JToken Value { get; set; }
		public PatchRequest[] Nested { get; set; }
		public string Name { get; set; }
		public int? Position { get; set; }

		public JObject ToJson()
		{
			return new JObject(
				new JProperty("Type", new JValue(Type)),
				new JProperty("PrevVal", PrevVal),
				new JProperty("Value", Value),
				new JProperty("Name", new JValue(Name)),
				new JProperty("Position", Position== null ? null : new JValue(Position.Value)),
				new JProperty("Nested", Nested == null ? null : new JArray(Nested.Select(x=>x.ToJson())))
				);
		}
	}
}