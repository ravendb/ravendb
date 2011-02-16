using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database
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
		JObject Metadata { get; set; }

		/// <summary>
		/// 	Gets or sets the key for the document
		/// </summary>
		/// <value>The key.</value>
		string Key { get; set; }

		/// <summary>
		/// 	Gets or sets a value indicating whether this document is non authoritive (modified by uncommitted transaction).
		/// </summary>
		bool NonAuthoritiveInformation { get; set; }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		Guid Etag { get; set; }

		/// <summary>
		/// 	Gets or sets the last modified date for the document
		/// </summary>
		/// <value>The last modified.</value>
		DateTime LastModified { get; set; }
	}
}