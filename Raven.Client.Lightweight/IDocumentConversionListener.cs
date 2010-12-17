using Newtonsoft.Json.Linq;

namespace Raven.Client
{
    /// <summary>
    /// Hook for users to provide additional logic for converting to / from 
    /// entities to document/metadata pairs.
    /// </summary>
    public interface IDocumentConversionListener
    {
        /// <summary>
        /// Called when converting an entity to a document and metadata
        /// </summary>
        void EntityToDocument(object entity, JObject document, JObject metadata);

        /// <summary>
        /// Called when converting a document and metadata to an entity
        /// </summary>
        void DocumentToEntity(object entity, JObject document, JObject metadata);

    }
}