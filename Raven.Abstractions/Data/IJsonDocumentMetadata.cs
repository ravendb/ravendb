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
		/// RavenJObject representing document's metadata.
		/// </summary>
		RavenJObject Metadata { get; set; }

		/// <summary>
		/// Key of a document.
		/// </summary>
		string Key { get; set; }

		/// <summary>
		/// Indicates whether this document is non authoritative (modified by uncommitted transaction).
		/// </summary>
		bool? NonAuthoritativeInformation { get; set; }

		/// <summary>
		/// Current document etag, used for concurrency checks (null to skip check)
		/// </summary>
		Etag Etag { get; set; }

		/// <summary>
		/// Last modified date for the document
		/// </summary>
		DateTime? LastModified { get; set; }
	}
}
