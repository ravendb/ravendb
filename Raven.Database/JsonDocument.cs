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
		public DateTime LastModified { get; set; }
		public JObject Projection { get; set; }

		public JObject ToJson()
		{
			if (Projection != null)
				return Projection;

			var doc = new JObject(DataAsJson);//clone the document
			var metadata = new JObject(Metadata);// clone the metadata
			metadata["Last-Modified"] = JToken.FromObject(LastModified.ToString("r"));
			var etagProp = metadata.Property("@etag");
			if (etagProp == null)
			{
				etagProp = new JProperty("@etag");
				metadata.Add(etagProp);
			}
			etagProp.Value = new JValue(Etag.ToString());
			doc.Add("@metadata", metadata);
			Metadata["Non-Authoritive-Information"] = JToken.FromObject(NonAuthoritiveInformation);
			return doc;
		}
	}
}