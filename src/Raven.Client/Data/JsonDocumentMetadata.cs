using System;
using Raven.Json.Linq;

namespace Raven.Client.Data
{
    /// <summary>
    /// A document representation:
    /// * Etag
    /// * Metadata
    /// </summary>
    public class JsonDocumentMetadata : IJsonDocumentMetadata
    {
        /// <summary>
        /// Metadata for the document
        /// </summary>
        public RavenJObject Metadata { get; set; }

        /// <summary>
        /// Key for the document
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Current document etag.
        /// </summary>
        public long? Etag { get; set; }

        /// <summary>
        /// Last modified date for the document
        /// </summary>
        public DateTime? LastModified { get; set; }
    }
}
