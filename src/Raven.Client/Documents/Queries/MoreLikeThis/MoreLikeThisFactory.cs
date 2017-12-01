using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public interface IMoreLikeThisFactoryBase<T>
    {
        IMoreLikeThisOperations<T> UsingAnyDocument();

        IMoreLikeThisOperations<T> UsingDocument(string documentJson);
    }

    public interface IMoreLikeThisFactory<T> : IMoreLikeThisFactoryBase<T>
    {
        IMoreLikeThisOperations<T> UsingDocument(Expression<Func<T, bool>> predicate);
    }

    public interface IMoreLikeThisFactoryForDocumentQuery<T> : IMoreLikeThisFactoryBase<T>
    {
        IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IDocumentQuery<T>>> predicate);
    }

    public interface IMoreLikeThisFactoryForAsyncDocumentQuery<T> : IMoreLikeThisFactoryBase<T>
    {
        IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> predicate);
    }

    public interface IMoreLikeThisOperations<T>
    {
        IMoreLikeThisOperations<T> WithOptions(MoreLikeThisOptions options);
    }

    internal class MoreLikeThisFactory<T> : IMoreLikeThisFactory<T>, IMoreLikeThisFactoryForDocumentQuery<T>, IMoreLikeThisFactoryForAsyncDocumentQuery<T>, IMoreLikeThisOperations<T>
    {
        public IMoreLikeThisOperations<T> UsingAnyDocument()
        {
            MoreLikeThis = new MoreLikeThisUsingAnyDocument();

            return this;
        }

        public IMoreLikeThisOperations<T> UsingDocument(string documentJson)
        {
            MoreLikeThis = new MoreLikeThisUsingDocument(documentJson);

            return this;
        }

        public IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IDocumentQuery<T>>> predicate)
        {
            MoreLikeThis = new MoreLikeThisUsingDocumentForDocumentQuery<T>
            {
                ForDocumentQuery = predicate
            };

            return this;
        }

        public IMoreLikeThisOperations<T> UsingDocument(Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> predicate)
        {
            MoreLikeThis = new MoreLikeThisUsingDocumentForDocumentQuery<T>
            {
                ForAsyncDocumentQuery = predicate
            };

            return this;
        }

        public IMoreLikeThisOperations<T> UsingDocument(Expression<Func<T, bool>> predicate)
        {
            MoreLikeThis = new MoreLikeThisUsingDocumentForQuery<T>
            {
                ForQuery = predicate
            };

            return this;
        }

        public IMoreLikeThisOperations<T> WithOptions(MoreLikeThisOptions options)
        {
            MoreLikeThis.Options = options;

            return this;
        }

        internal MoreLikeThisBase MoreLikeThis { get; private set; }
    }
}
