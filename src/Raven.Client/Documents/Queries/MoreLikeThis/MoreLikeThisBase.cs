using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public abstract class MoreLikeThisBase
    {
        public MoreLikeThisOptions Options { get; set; }
    }

    internal class MoreLikeThisUsingDocumentForQuery<T> : MoreLikeThisBase
    {
        public Expression<Func<T, bool>> ForQuery { get; set; }
    }

    internal class MoreLikeThisUsingDocumentForDocumentQuery<T> : MoreLikeThisBase
    {
        public Action<IFilterDocumentQueryBase<T, IDocumentQuery<T>>> ForDocumentQuery { get; set; }

        public Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> ForAsyncDocumentQuery { get; set; }
    }

    public class MoreLikeThisUsingAnyDocument : MoreLikeThisBase
    {
    }

    public class MoreLikeThisUsingDocument : MoreLikeThisBase
    {
        public MoreLikeThisUsingDocument(string documentJson)
        {
            DocumentJson = documentJson ?? throw new ArgumentNullException(nameof(documentJson));
        }

        public string DocumentJson { get; set; }
    }
}
