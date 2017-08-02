using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Linq
{
    ///<summary>
    /// Generate a new document query
    ///</summary>
    public interface IDocumentQueryGenerator
    {
        /// <summary>
        /// Gets the conventions associated with this query
        /// </summary>
        DocumentConventions Conventions { get; }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> Query<T>(string indexName, string collectionName, bool isMapReduce);

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, string collectionName, bool isMapReduce);

        /// <summary>
        /// Generates a query inspector
        /// </summary>
        /// <returns>RavenQueryInspector object</returns>
        RavenQueryInspector<TS> CreateRavenQueryInspector<TS>();
    }

}
