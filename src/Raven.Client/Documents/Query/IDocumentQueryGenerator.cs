using Raven.Client.Document;
using Raven.Client.Linq;

namespace Raven.Client.Documents
{
    ///<summary>
    /// Generate a new document query
    ///</summary>
    public interface IDocumentQueryGenerator
    {
        /// <summary>
        /// Gets the conventions associated with this query
        /// </summary>
        DocumentConvention Conventions { get; }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> Query<T>(string indexName, bool isMapReduce);

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce);

        /// <summary>
        /// Generates a query inspector
        /// </summary>
        /// <returns>RavenQueryInspector object</returns>
        RavenQueryInspector<S> CreateRavenQueryInspector<S>();
    }

}
