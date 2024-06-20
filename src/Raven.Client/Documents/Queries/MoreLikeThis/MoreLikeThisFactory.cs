using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    /// <inheritdoc cref="IMoreLikeThisOperations{T}"/>
    public interface IMoreLikeThisBuilderBase<T>
    {
        IMoreLikeThisOperations<T> UsingAnyDocument();

        /// <inheritdoc cref="IMoreLikeThisOperations{T}"/>
        /// <param name="documentJson">Inline JSON document that will be used as a base for operation. </param>
        IMoreLikeThisOperations<T> UsingDocument(string documentJson);
    }

    /// <inheritdoc />
    public interface IMoreLikeThisBuilder<T> : IMoreLikeThisBuilderBase<T>
    {
        /// <inheritdoc cref="IMoreLikeThisOperations{T}"/>
        /// <param name="predicate">Filtering expression utilized to find a document that will be used as a base for operation. </param>
        IMoreLikeThisOperations<T> UsingDocument(Expression<Func<T, bool>> predicate);
    }

    /// <inheritdoc />
    public interface IMoreLikeThisBuilderForDocumentQuery<T> : IMoreLikeThisBuilderBase<T>
    {
        /// <inheritdoc cref="IMoreLikeThisOperations{T}"/>
        /// <param name="predicate">Filtering expression utilized to find a document that will be used as a base for operation.</param>
        IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IDocumentQuery<T>>> predicate);
    }

    /// <inheritdoc />
    public interface IMoreLikeThisBuilderForAsyncDocumentQuery<T> : IMoreLikeThisBuilderBase<T>
    {
        /// <inheritdoc cref="IMoreLikeThisBuilderForDocumentQuery{T}.UsingDocument(System.Action{Raven.Client.Documents.Session.IFilterDocumentQueryBase{T,Raven.Client.Documents.Session.IDocumentQuery{T}}})"/>
        IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> predicate);
    }

    /// <summary>
    /// Get similar documents according to the provided criteria and options.
    /// </summary>
    /// <typeparam name="T">Queried document type.</typeparam>
    /// <inheritdoc cref="DocumentationUrls.Session.Querying.MoreLikeThisQuery"/>
    public interface IMoreLikeThisOperations<T>
    {
        /// <summary>
        /// Add custom parameters to your MoreLikeThis query
        /// </summary>
        /// <param name="options">Configure custom options for your query. See more at: <see cref="MoreLikeThisOptions"/></param>
        /// <returns></returns>
        IMoreLikeThisOperations<T> WithOptions(MoreLikeThisOptions options);
    }

    internal sealed class MoreLikeThisBuilder<T> : IMoreLikeThisBuilder<T>, IMoreLikeThisBuilderForDocumentQuery<T>, IMoreLikeThisBuilderForAsyncDocumentQuery<T>, IMoreLikeThisOperations<T>
    {
        /// <inheritdoc/>
        public IMoreLikeThisOperations<T> UsingAnyDocument()
        {
            MoreLikeThis = new MoreLikeThisUsingAnyDocument();

            return this;
        }

        /// <inheritdoc/>
        public IMoreLikeThisOperations<T> UsingDocument(string documentJson)
        {
            MoreLikeThis = new MoreLikeThisUsingDocument(documentJson);

            return this;
        }

        /// <inheritdoc cref="IMoreLikeThisBuilderForDocumentQuery{T}.UsingDocument(System.Action{Raven.Client.Documents.Session.IFilterDocumentQueryBase{T,Raven.Client.Documents.Session.IDocumentQuery{T}}})" />
        public IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IDocumentQuery<T>>> predicate)
        {
            MoreLikeThis = new MoreLikeThisUsingDocumentForDocumentQuery<T>
            {
                ForDocumentQuery = predicate
            };

            return this;
        }

        /// <inheritdoc/>
        public IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> predicate)
        {
            MoreLikeThis = new MoreLikeThisUsingDocumentForDocumentQuery<T>
            {
                ForAsyncDocumentQuery = predicate
            };

            return this;
        }

        /// <inheritdoc/>
        public IMoreLikeThisOperations<T> UsingDocument(Expression<Func<T, bool>> predicate)
        {
            MoreLikeThis = new MoreLikeThisUsingDocumentForQuery<T>
            {
                ForQuery = predicate
            };

            return this;
        }

        /// <inheritdoc/>
        public IMoreLikeThisOperations<T> WithOptions(MoreLikeThisOptions options)
        {
            MoreLikeThis.Options = options;

            return this;
        }

        internal MoreLikeThisBase MoreLikeThis { get; private set; }
    }
}
