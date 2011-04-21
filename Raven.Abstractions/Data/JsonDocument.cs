//-----------------------------------------------------------------------
// <copyright file="JsonDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
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
		public bool? NonAuthoritiveInformation { get; set; }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid? Etag { get; set; }

		/// <summary>
		/// 	Gets or sets the last modified date for the document
		/// </summary>
		/// <value>The last modified.</value>
		public DateTime? LastModified { get; set; }

		/// <summary>
		/// 	Translate the json document to a <see cref = "RavenJObject" />
		/// </summary>
		/// <returns></returns>
		public RavenJObject ToJson()
		{

			var doc = (RavenJObject)DataAsJson.CloneToken();
			var metadata = (RavenJObject)Metadata.CloneToken();

			if (LastModified != null)
				metadata["Last-Modified"] = LastModified.Value.ToString("r");
			if(Etag != null)
				metadata["@etag"] = Etag.Value.ToString();
			if (NonAuthoritiveInformation != null)
				metadata["Non-Authoritive-Information"] = NonAuthoritiveInformation.Value;

			doc["@metadata"] = metadata;

			return doc;
		}
	}
}
