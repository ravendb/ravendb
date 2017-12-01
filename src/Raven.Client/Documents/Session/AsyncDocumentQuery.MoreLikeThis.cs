using System;
using Raven.Client.Documents.Queries.MoreLikeThis;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.MoreLikeThis(MoreLikeThisBase moreLikeThis)
        {
            using (var mlt = MoreLikeThis())
            {
                mlt.WithOptions(moreLikeThis.Options);

                if (moreLikeThis is MoreLikeThisUsingDocument document)
                    mlt.WithDocument(document.DocumentJson);
            }

            return this;
        }

        IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.MoreLikeThis(Action<IMoreLikeThisFactoryForAsyncDocumentQuery<T>> factory)
        {
            var f = new MoreLikeThisFactory<T>();
            factory.Invoke(f);

            using (var moreLikeThis = MoreLikeThis())
            {
                moreLikeThis.WithOptions(f.MoreLikeThis.Options);

                if (f.MoreLikeThis is MoreLikeThisUsingDocument document)
                    moreLikeThis.WithDocument(document.DocumentJson);
                else if (f.MoreLikeThis is MoreLikeThisUsingDocumentForDocumentQuery<T> query)
                    query.ForAsyncDocumentQuery(this);
            }

            return this;
        }
    }
}
