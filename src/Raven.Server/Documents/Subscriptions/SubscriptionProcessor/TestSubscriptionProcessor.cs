using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public class TestDocumentsSubscriptionProcessor : DocumentsSubscriptionProcessor, IEtagSettable
    {
        public TestDocumentsSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionState state, string collection, SubscriptionWorkerOptions options, EndPoint endpoint) : 
            base(server, database, connection: null)
        {
            SubscriptionState = state;
            Collection = collection;
            Options = options;
            RemoteEndpoint = endpoint;
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

        public override Task AcknowledgeBatch(long batchId)
        {
            throw new NotSupportedException();
        }
    }

    public class TestRevisionsSubscriptionProcessor : RevisionsSubscriptionProcessor, IEtagSettable
    {
        public TestRevisionsSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionState state, string collection, SubscriptionWorkerOptions options, EndPoint endpoint) :
            base(server, database, connection: null)
        {
            SubscriptionState = state;
            Collection = collection;
            Options = options;
            RemoteEndpoint = endpoint;
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

        public override Task AcknowledgeBatch(long batchId)
        {
            throw new NotSupportedException();
        }
    }

    public interface IEtagSettable
    {
        public void SetStartEtag(long etag);
    }
}
