using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    /// <inheritdoc cref="IMoreLikeThisOperations{T}"/>
    public abstract class MoreLikeThisBase
    {
        /// <inheritdoc cref="MoreLikeThisOptions"/>
        public MoreLikeThisOptions Options { get; set; }
    }

    /// <inheritdoc/>
    internal sealed class MoreLikeThisUsingDocumentForQuery<T> : MoreLikeThisBase
    {
        public Expression<Func<T, bool>> ForQuery { get; set; }
    }

    /// <inheritdoc/>
    internal sealed class MoreLikeThisUsingDocumentForDocumentQuery<T> : MoreLikeThisBase
    {
        /// <summary>
        /// Specify document for MoreLikeThis query by <see cref="IDocumentQuery{T}">IDocumentQuery</see> operations.
        /// </summary>
        public Action<IFilterDocumentQueryBase<T, IDocumentQuery<T>>> ForDocumentQuery { get; set; }

        /// <summary>
        /// Specify document for MoreLikeThis query by <see cref="IAsyncDocumentQuery{T}">IAsyncDocumentQuery</see> operations.
        /// </summary>
        public Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> ForAsyncDocumentQuery { get; set; }
    }

    /// <inheritdoc/>
    public sealed class MoreLikeThisUsingAnyDocument : MoreLikeThisBase
    {
    }

    /// <inheritdoc/>
    public sealed class MoreLikeThisUsingDocument : MoreLikeThisBase
    {
        /// <inheritdoc cref="IMoreLikeThisBuilderBase{T}.UsingDocument"/>
        public MoreLikeThisUsingDocument(string documentJson)
        {
            DocumentJson = documentJson ?? throw new ArgumentNullException(nameof(documentJson));
        }

        /// <summary>
        /// JSON document that will be used as a base for operation.
        /// </summary>
        public string DocumentJson { get; set; }
    }
}
