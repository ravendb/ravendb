//-----------------------------------------------------------------------
// <copyright file="JsonDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Data
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
        }

        private RavenJObject dataAsJson;
        private RavenJObject metadata;

        /// <summary>
        /// Document data or projection as json.
        /// </summary>
        public RavenJObject DataAsJson
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return dataAsJson ?? (dataAsJson = new RavenJObject()); }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { dataAsJson = value; }
        }

        /// <summary>
        /// Metadata for the document
        /// </summary>		
        public RavenJObject Metadata
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return metadata ?? (metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase)); }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { metadata = value; }
        }

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

        /// <summary>
        /// The ranking of this result in the current query
        /// </summary>
        public float? TempIndexScore { get; set; }

        /// <summary>
        /// How much space this document takes on disk
        /// Only relevant during indexing phases, and not available on the client
        /// </summary>
        public int SerializedSizeOnDisk;

        /// <summary>
        /// Whatever this document can be skipped from delete
        /// Only relevant during indexing phases, and not available on the client
        /// </summary>
        public bool SkipDeleteFromIndex;

        /// <summary>
        /// Translate the json document to a <see cref = "RavenJObject" />
        /// </summary>
        public RavenJObject ToJson(bool checkForId = false)
        {
            DataAsJson.EnsureCannotBeChangeAndEnableSnapshotting();
            Metadata.EnsureCannotBeChangeAndEnableSnapshotting();

            var doc = (RavenJObject)DataAsJson.CreateSnapshot();
            var metadata = (RavenJObject)Metadata.CreateSnapshot();

            if (LastModified != null)
            {
                metadata[Constants.Headers.LastModified] = LastModified.Value;
                metadata[Constants.Headers.RavenLastModified] = LastModified.Value.GetDefaultRavenFormat();
            }
            if (Etag != null)
                metadata["@etag"] = Etag.ToString();
            if (checkForId && metadata.ContainsKey("@id") == false)
                metadata["@id"] = Key;
            doc["@metadata"] = metadata;

            return doc;
        }

        public override string ToString()
        {
            return Key;
        }

        public static void EnsureIdInMetadata(IJsonDocumentMetadata doc)
        {
            if (doc == null || doc.Metadata == null)
                return;

            if (doc.Metadata.IsSnapshot)
            {
                doc.Metadata = (RavenJObject)doc.Metadata.CreateSnapshot();
            }

            doc.Metadata["@id"] = doc.Key;
        }
    }
}
