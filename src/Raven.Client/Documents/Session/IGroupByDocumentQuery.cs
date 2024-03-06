using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Perform dynamic group by on collection data. AutoMapReduce index is created if none of the indexes in the database are sufficient to match the query criteria.
    /// </summary>
    /// <typeparam name="T">Document type.</typeparam>
    /// <inheritdoc cref="DocumentationUrls.Session.Querying.GroupByQuery"/>
    public interface IGroupByDocumentQuery<T>
    {
        /// <summary>
        /// Include group-by key in query projection.
        /// </summary>
        /// <param name="fieldName">GroupBy field name</param>
        /// <param name="projectedName">Projection alias.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.GroupByQuery"/>
        IGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null);

        /// <summary>
        /// Sum elements for a specific group-by key.
        /// </summary>
        /// <param name="field">GroupBy field to sum.</param>
        /// <param name="fields">Additional fields to sum.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.GroupByQuery"/>
        IDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields);

        /// <summary>
        /// Get count of elements in GroupBy key.
        /// </summary>
        /// <param name="projectedName">Set alias for field with count value. (Default: 'Count')</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.GroupByQuery"/>
        IDocumentQuery<T> SelectCount(string projectedName = null);

        /// <summary>
        /// Filter allows querying on documents without the need for issuing indexes. It is meant for exploratory queries or post query filtering. Criteria are evaluated at query time so please use Filter wisely to avoid performance issues.
        /// </summary>
        /// <param name="builder">Builder of a Filter query</param>
        /// <param name="limit">Limits the number of documents processed by Filter.</param>
        IGroupByDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit = int.MaxValue);
    }

    /// <inheritdoc cref="IGroupByDocumentQuery{T}"/>
    public interface IAsyncGroupByDocumentQuery<T>
    {
        /// <inheritdoc cref="IGroupByDocumentQuery{T}.SelectKey"/>
        IAsyncGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null);

        /// <inheritdoc cref="IGroupByDocumentQuery{T}.SelectSum"/>
        IAsyncDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields);

        /// <inheritdoc cref="IGroupByDocumentQuery{T}.SelectCount"/>
        IAsyncDocumentQuery<T> SelectCount(string projectedName = null);

        /// <inheritdoc cref="IGroupByDocumentQuery{T}.Filter"/>
        IAsyncGroupByDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit = int.MaxValue);
    }
}
