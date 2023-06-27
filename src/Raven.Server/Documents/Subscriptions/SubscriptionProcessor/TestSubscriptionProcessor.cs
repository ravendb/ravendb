using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public class TestDocumentsDatabaseSubscriptionProcessor : DocumentsDatabaseSubscriptionProcessor, IEtagSettable
    {
        private readonly SubscriptionConnection.ParsedSubscription _subscription;

        public TestDocumentsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionState state, SubscriptionConnection.ParsedSubscription subscription, SubscriptionWorkerOptions options, EndPoint endpoint) :
            base(server, database, connection: null)
        {
            _subscription = subscription;
            SubscriptionState = state;
            Collection = subscription.Collection;
            Options = options;
            RemoteEndpoint = endpoint;
        }

        protected override DatabaseIncludesCommandImpl CreateIncludeCommands()
        {
            var includes = CreateIncludeCommandsInternal(Database, DocsContext, Connection, _subscription);
            return includes;
        }

        public override void InitializeProcessor()
        {
            SubscriptionConnectionsState = SubscriptionConnectionsState.CreateDummyState(Database.DocumentsStorage, SubscriptionState);
        }

        public void SetStartEtag(long etag)
        {
            Fetcher.StartEtag = etag;
        }

        public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
        {
            throw new NotSupportedException();
        }

        public override Task AcknowledgeBatch(long batchId, string changeVector)
        {
            throw new NotSupportedException();
        }
    }

    public class TestRevisionsDatabaseSubscriptionProcessor : RevisionsDatabaseSubscriptionProcessor, IEtagSettable
    {
        private readonly SubscriptionConnection.ParsedSubscription _subscription;

        public TestRevisionsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionState state, SubscriptionConnection.ParsedSubscription subscription, SubscriptionWorkerOptions options, EndPoint endpoint) :
            base(server, database, connection: null)
        {
            _subscription = subscription;
            SubscriptionState = state;
            Collection = subscription.Collection;
            Options = options;
            RemoteEndpoint = endpoint;
        }

        public override void InitializeProcessor()
        {
            SubscriptionConnectionsState = SubscriptionConnectionsState.CreateDummyState(Database.DocumentsStorage, SubscriptionState);
        }

        protected override DatabaseIncludesCommandImpl CreateIncludeCommands()
        {
            var includes = CreateIncludeCommandsInternal(Database, DocsContext, Connection, _subscription);
            return includes;
        }

        public void SetStartEtag(long etag)
        {
            Fetcher.StartEtag = etag;
        }

        public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
        {
            throw new NotSupportedException();
        }

        public override Task AcknowledgeBatch(long batchId, string changeVector)
        {
            throw new NotSupportedException();
        }
    }

    public interface IEtagSettable
    {
        public void SetStartEtag(long etag);
    }
}
