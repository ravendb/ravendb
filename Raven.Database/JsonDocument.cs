using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database
{
	/// <summary>
	/// A document representation:
	/// * Data / Projection
	/// * Etag
	/// * Metadata
	/// </summary>
	public class JsonDocument
	{
		/// <summary>
		/// 	Gets or sets the document data as json.
		/// </summary>
		/// <value>The data as json.</value>
		public JObject DataAsJson { get; set; }

		/// <summary>
		/// 	Gets or sets the metadata for the document
		/// </summary>
		/// <value>The metadata.</value>
		public JObject Metadata { get; set; }

		/// <summary>
		/// 	Gets or sets the key for the document
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }

		/// <summary>
		/// 	Gets or sets a value indicating whether this document is non authoritive (modified by uncommited transaction).
		/// </summary>
		public bool NonAuthoritiveInformation { get; set; }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid Etag { get; set; }

		/// <summary>
		/// 	Gets or sets the last modified date for the document
		/// </summary>
		/// <value>The last modified.</value>
		public DateTime LastModified { get; set; }

		/// <summary>
		/// 	Gets or sets the projection for this document. The projection is used when loading the data directly from the index.
		/// 	Either <see cref = "Projection" /> or <see cref = "DataAsJson" /> are valid, never both.
		/// </summary>
		/// <value>The projection.</value>
		public JObject Projection { get; set; }

		/// <summary>
		/// 	Translate the json document to a <see cref = "JObject" />
		/// </summary>
		/// <returns></returns>
		public JObject ToJson()
		{
			if (Projection != null)
				return Projection;

			var doc = new JObject(DataAsJson); //clone the document
			var metadata = new JObject(Metadata); // clone the metadata
			metadata["Last-Modified"] = JToken.FromObject(LastModified.ToString("r"));
			var etagProp = metadata.Property("@etag");
			if (etagProp == null)
			{
				etagProp = new JProperty("@etag");
				metadata.Add(etagProp);
			}
			etagProp.Value = new JValue(Etag.ToString());
			doc.Add("@metadata", metadata);
			metadata["Non-Authoritive-Information"] = JToken.FromObject(NonAuthoritiveInformation);
			return doc;
		}
	}
}