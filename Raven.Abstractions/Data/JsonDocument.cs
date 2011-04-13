//-----------------------------------------------------------------------
// <copyright file="JsonDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Json.Linq;

namespace Raven.Database
{
	/// <summary>
	/// A document representation:
	/// * Data / Projection
	/// * Etag
	/// * Metadata
	/// </summary>
	public class JsonDocument : IJsonDocumentMetadata
	{
		/// <summary>
		/// Create a new instance of JsonDocument
		/// </summary>
		public JsonDocument()
		{
			DataAsJson = new RavenJObject();
			Metadata = new RavenJObject();
		}
		/// <summary>
		/// 	Gets or sets the document data as json.
		/// </summary>
		/// <value>The data as json.</value>
		public RavenJObject DataAsJson { get; set; }

		/// <summary>
		/// 	Gets or sets the metadata for the document
		/// </summary>
		/// <value>The metadata.</value>
		public RavenJObject Metadata { get; set; }

		/// <summary>
		/// 	Gets or sets the key for the document
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }

		/// <summary>
		/// 	Gets or sets a value indicating whether this document is non authoritive (modified by uncommitted transaction).
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
		public RavenJObject Projection { get; set; }

		/// <summary>
		/// 	Translate the json document to a <see cref = "RavenJObject" />
		/// </summary>
		/// <returns></returns>
		public RavenJObject ToJson()
		{
			if (Projection != null)
				return Projection;


			var doc = DataAsJson.CloneToken() as RavenJObject;
			var metadata = Metadata.CloneToken();
			metadata["Last-Modified"] = LastModified;
			metadata["@etag"] = Etag.ToString();
			doc["@metadata"] = metadata;
			metadata["Non-Authoritive-Information"] = RavenJToken.FromObject(NonAuthoritiveInformation);

			return doc;
		}
	}
}
