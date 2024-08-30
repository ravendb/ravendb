using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow.Logging;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionFetcher<T> : SubscriptionFetcher
    {
        protected Logger Logger;

        protected SubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : base(database, subscriptionConnectionsState, collection)
        {
            Logger = LoggingSource.Instance.GetLogger<SubscriptionFetcher<T>>(Database.Name);
        }

        protected abstract IEnumerator<T> FetchByEtag();

        protected abstract IEnumerator<T> FetchFromResend();

        public IEnumerable<T> GetEnumerator()
        {
            FetchingFrom = FetchingOrigin.Resend;
            foreach (var item in FetchFromResend())
            {
                yield return item;
            }

            if (DocSent)
            {
                // we don't mix resend and regular, so we need to do another round when we are done with the resend
                SubscriptionConnectionsState.NotifyHasMoreDocs();
                yield break;
            }

            FetchingFrom = FetchingOrigin.Storage;
            foreach (var item in FetchByEtag())
            {
                yield return item;
            }
        }
    }

    public abstract class SubscriptionFetcher
    {
        protected readonly DocumentDatabase Database;
        protected readonly SubscriptionConnectionsState SubscriptionConnectionsState;
        protected readonly string Collection;

        protected SubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection)
        {
            Database = database;
            SubscriptionConnectionsState = subscriptionConnectionsState;
            Collection = collection;
        }

        protected ClusterOperationContext ClusterContext;
        protected DocumentsOperationContext DocsContext;
        protected HashSet<long> Active;
        protected internal long StartEtag;

        public virtual void Initialize(
            ClusterOperationContext clusterContext,
            DocumentsOperationContext docsContext,
            HashSet<long> active)
        {
            ClusterContext = clusterContext;
            DocsContext = docsContext;
            Active = active;
            StartEtag = SubscriptionConnectionsState.GetLastEtagSent();
            DocSent = false;
        }

        public enum FetchingOrigin
        {
            None,
            Resend,
            Storage
        }

        protected bool DocSent;

        public void MarkDocumentSent()
        {
            DocSent = true;
        }

        public FetchingOrigin FetchingFrom;
    }

    public class RevisionSubscriptionFetcher : SubscriptionFetcher<(Document Previous, Document Current)>
    {
        public RevisionSubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : base(database, subscriptionConnectionsState, collection)
        {

        }

        protected override IEnumerator<(Document Previous, Document Current)> FetchByEtag()
        {
            return Collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection => new TransactionForgetAboutCurrentPreviousRevisionEnumerator(
                    Database.DocumentsStorage.RevisionsStorage.GetCurrentAndPreviousRevisionsForSubscriptionsFrom(DocsContext, StartEtag + 1, 0, long.MaxValue)
                        .GetEnumerator(), DocsContext),
                _ => new TransactionForgetAboutCurrentPreviousRevisionEnumerator(
                    Database.DocumentsStorage.RevisionsStorage
                        .GetCurrentAndPreviousRevisionsForSubscriptionsFrom(DocsContext, new CollectionName(Collection), StartEtag + 1, long.MaxValue)
                        .GetEnumerator(), DocsContext)
            };
        }

        protected override IEnumerator<(Document Previous, Document Current)> FetchFromResend()
        {
            return new TransactionForgetAboutCurrentPreviousRevisionEnumerator(SubscriptionConnectionsState.GetRevisionsFromResend(Database, ClusterContext, DocsContext, Active)
                .GetEnumerator(), DocsContext);
        }
    }

    public class DocumentSubscriptionFetcher : SubscriptionFetcher<Document>
    {
        public DocumentSubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) :
            base(database, subscriptionConnectionsState, collection)
        {
        }

        protected override IEnumerator<Document> FetchByEtag()
        {
            return Collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection =>
                    new TransactionForgetAboutDocumentEnumerator(Database.DocumentsStorage.GetDocumentsFrom(DocsContext, StartEtag + 1, 0, long.MaxValue)
                        .GetEnumerator(), DocsContext),
                _ =>
                    new TransactionForgetAboutDocumentEnumerator(Database.DocumentsStorage.GetDocumentsFrom(
                        DocsContext,
                        Collection,
                        StartEtag + 1,
                        0,
                        long.MaxValue)
                        .GetEnumerator(), DocsContext)
            };
        }

        protected override IEnumerator<Document> FetchFromResend()
        {
            foreach (var record in SubscriptionConnectionsState.GetDocumentsFromResend(ClusterContext, Active))
            {
                yield return new Document
                {
                    Id = DocsContext.GetLazyString(record.DocumentId),
                    ChangeVector = record.ChangeVector
                };
            }
        }
    }
}
