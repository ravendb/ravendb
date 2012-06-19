using System;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// A document representation:
	/// * Etag
	/// * Metadata
	/// </summary>
	public class JsonDocumentMetadata : IJsonDocumentMetadata
	{
		/// <summary>
		/// Gets or sets the metadata for the document
		/// </summary>
		/// <value>The metadata.</value>
		public RavenJObject Metadata { get; set; }

		/// <summary>
		/// Gets or sets the key for the document
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether this document is non authoritative (modified by uncommitted transaction).
		/// </summary>
		public bool? NonAuthoritativeInformation { get; set; }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid? Etag { get; set; }

		/// <summary>
		/// Gets or sets the last modified date for the document
		/// </summary>
		/// <value>The last modified.</value>
		public DateTime? LastModified { get; set; }
	}
}