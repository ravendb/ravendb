using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database
{
	public class JsonDocument
	{
		public JObject DataAsJson { get; set; }
		public JObject Metadata { get; set; }
		public string Key { get; set; }
		public bool NonAuthoritiveInformation { get; set; }
		public Guid Etag { get; set; }

		public JObject Projection { get; set; }

		public JObject ToJson()
		{
			if (Projection != null)
				return Projection;

			var doc = DataAsJson;
			var etagProp = Metadata.Property("@etag");
			if (etagProp == null)
			{
				etagProp = new JProperty("@etag");
				Metadata.Add(etagProp);
			}
			etagProp.Value = new JValue(Etag.ToString());
			doc.Add("@metadata", Metadata);
			Metadata["Non-Authoritive-Information"] = JToken.FromObject(NonAuthoritiveInformation);
			return doc;
		}
	}
}