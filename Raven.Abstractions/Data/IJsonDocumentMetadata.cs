using System;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Interface that is used purely internally
	/// </summary>
	public interface IJsonDocumentMetadata
	{
		/// <summary>
		/// 	Gets or sets the metadata for the document
		/// </summary>
		/// <value>The metadata.</value>
		RavenJObject Metadata { get; set; }

		/// <summary>
		/// 	Gets or sets the key for the document
		/// </summary>
		/// <value>The key.</value>
		string Key { get; set; }

		/// <summary>
		/// 	Gets or sets a value indicating whether this document is non authoritative (modified by uncommitted transaction).
		/// </summary>
		bool? NonAuthoritativeInformation { get; set; }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		Etag Etag { get; set; }

		/// <summary>
		/// 	Gets or sets the last modified date for the document
		/// </summary>
		/// <value>The last modified.</value>
		DateTime? LastModified { get; set; }
	}
}
