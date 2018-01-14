using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Information held about an entity by the session
    /// </summary>
    public class DocumentInfo
    {
        /// <summary>
        /// Gets or sets the id.
        /// </summary>
        /// <value>The id.</value>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the ChangeVector.
        /// </summary>
        /// <value>The ChangeVector.</value>
        public string ChangeVector { get; set; }

        /// <summary>
        /// A concurrency check will be forced on this entity 
        /// even if UseOptimisticConcurrency is set to false
        /// </summary>
        public ConcurrencyCheckMode ConcurrencyCheckMode { get; set; }

        /// <summary>
        /// If set to true, the session will ignore this document
        /// when SaveChanges() is called, and won't perform and change tracking
        /// </summary>
        public bool IgnoreChanges { get; set; }

        public BlittableJsonReaderObject Metadata { get; set; }

        public BlittableJsonReaderObject Document { get; set; }

        public IMetadataDictionary MetadataInstance { get; set; }

        public object Entity { get; set; }

        public bool IsNewDocument { get; set; }

        public string Collection { get; set; }

        public DateTime GetLastModified()
        {
            if (Metadata.TryGet(Constants.Documents.Metadata.LastModified, out DateTime lastModified) == false)
                throw new InvalidOperationException($"Document {Id} must have a last modified field");

            return lastModified;
        }

        public static DocumentInfo GetNewDocumentInfo(BlittableJsonReaderObject document)
        {
            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                throw new InvalidOperationException("Document must have a metadata");
            if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
                throw new InvalidOperationException("Document must have an id");
            if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false)
                throw new InvalidOperationException($"Document {id} must have a Change Vector");

            var newDocumentInfo = new DocumentInfo
            {
                Id = id,
                Document = document,
                Metadata = metadata,
                Entity = null,
                ChangeVector = changeVector
            };
            return newDocumentInfo;
        }
    }
}
