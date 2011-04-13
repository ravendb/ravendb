#if !NET_3_5
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client
{
    ///<summary>
    /// Asyncronous query against a raven index
    ///</summary>
    public interface IAsyncDocumentQuery<T> : IDocumentQueryBase<T, IAsyncDocumentQuery<T>>
    {

        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="fields">The fields.</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

        /// <summary>
        /// Gets the query result
        /// Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        Task<QueryResult> QueryResultAsync { get; }
    }
}
#endif
