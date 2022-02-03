using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    public interface IGroupByDocumentQuery<T>
    {
        IGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null);

        IDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields);

        IDocumentQuery<T> SelectCount(string projectedName = null);

        /// <summary>
        /// Filter allows querying on documents without the need for issuing indexes. It is meant for exploratory queries or post query filtering. Criteria are evaluated at query time so please use Filter wisely to avoid performance issues.
        /// </summary>
        /// <param name="builder">Builder of a Filter query</param>
        /// <param name="limit">Limits the number of documents processed by Filter.</param>
        /// <returns></returns>
        IGroupByDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit = int.MaxValue);
    }

    public interface IAsyncGroupByDocumentQuery<T>
    {
        IAsyncGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null);

        IAsyncDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields);

        IAsyncDocumentQuery<T> SelectCount(string projectedName = null);

        /// <summary>
        /// Filter allows querying on documents without the need for issuing indexes. It is meant for exploratory queries or post query filtering. Criteria are evaluated at query time so please use Filter wisely to avoid performance issues.
        /// </summary>
        /// <param name="builder">Builder of a Filter query</param>
        /// <param name="limit">Limits the number of documents processed by Filter.</param>
        /// <returns></returns>
        IAsyncGroupByDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit = int.MaxValue);
    }
}
