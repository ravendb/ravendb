using System;
using Raven.Client.Documents.Queries.MoreLikeThis;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        public IAsyncDocumentQuery<T> MoreLikeThis(MoreLikeThisOptions options = null)
        {
            using (var moreLikeThis = base.MoreLikeThis())
            {
                moreLikeThis.WithOptions(options);
            }

            return this;
        }

        public IAsyncDocumentQuery<T> MoreLikeThis(string document, MoreLikeThisOptions options = null)
        {
            using (var moreLikeThis = base.MoreLikeThis())
            {
                moreLikeThis.WithDocument(document);
                moreLikeThis.WithOptions(options);
            }

            return this;
        }

        public IAsyncDocumentQuery<T> MoreLikeThis(Action<IFilterDocumentQueryBase<T, IAsyncDocumentQuery<T>>> predicate, MoreLikeThisOptions options = null)
        {
            using (var moreLikeThis = base.MoreLikeThis())
            {
                moreLikeThis.WithOptions(options);

                predicate(this);
            }

            return this;
        }
    }
}
