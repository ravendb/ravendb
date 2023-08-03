using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Subscriptions.Processor
{
    public sealed class TestDocumentsDatabaseSubscriptionProcessor : DocumentsDatabaseSubscriptionProcessor, IEtagSettable
    {
        private readonly SubscriptionConnection.ParsedSubscription _subscription;
        private readonly TimeSpan _timeLimit;
        private readonly int _pageSize;

        public TestDocumentsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionState state, SubscriptionConnection.ParsedSubscription subscription, SubscriptionWorkerOptions options, EndPoint endpoint, TimeSpan timeLimit, int pageSize) :
            base(server, database, connection: null)
        {
            _subscription = subscription;
            _timeLimit = timeLimit;
            _pageSize = pageSize;
            SubscriptionState = state;
            Collection = subscription.Collection;
            Options = options;
            RemoteEndpoint = endpoint;
        }

        protected override void HandleBatchItem(SubscriptionBatchStatsScope batchScope, SubscriptionBatchItem batchItem, SubscriptionBatchResult result, Document item)
        {
            result.CurrentBatch.Add(batchItem);
        }

        protected override bool CanContinueBatch(SubscriptionBatchItemStatus batchItemStatus, SubscriptionBatchStatsScope batchScope, int numberOfDocs, Stopwatch sendingCurrentBatchStopwatch)
        {
            if (sendingCurrentBatchStopwatch.Elapsed > _timeLimit)
                return false;

            if (numberOfDocs >= _pageSize)
                return false;

            return true;
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

        public override Task<long> TryRecordBatchAsync(string lastChangeVectorSentInThisBatch)
        {
            throw new NotSupportedException();
        }

        public override Task AcknowledgeBatchAsync(long batchId, string changeVector)
        {
            throw new NotSupportedException();
        }
    }

    public sealed class TestRevisionsDatabaseSubscriptionProcessor : RevisionsDatabaseSubscriptionProcessor, IEtagSettable
    {
        private readonly SubscriptionConnection.ParsedSubscription _subscription;
        private readonly TimeSpan _timeLimit;
        private readonly int _pageSize;

        public TestRevisionsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionState state, SubscriptionConnection.ParsedSubscription subscription, SubscriptionWorkerOptions options, EndPoint endpoint, TimeSpan timeLimit, int pageSize) :
            base(server, database, connection: null)
        {
            _subscription = subscription;
            _timeLimit = timeLimit;
            _pageSize = pageSize;
            SubscriptionState = state;
            Collection = subscription.Collection;
            Options = options;
            RemoteEndpoint = endpoint;
        }

        protected override void HandleBatchItem(SubscriptionBatchStatsScope batchScope, SubscriptionBatchItem batchItem, SubscriptionBatchResult result, (Document Previous, Document Current) item)
        {
            result.CurrentBatch.Add(batchItem);
        }

        protected override bool CanContinueBatch(SubscriptionBatchItemStatus batchItemStatus, SubscriptionBatchStatsScope batchScope, int numberOfDocs, Stopwatch sendingCurrentBatchStopwatch)
        {
            if (sendingCurrentBatchStopwatch.Elapsed > _timeLimit)
                return false;

            if (numberOfDocs >= _pageSize)
                return false;

            return true;
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

        public override Task<long> TryRecordBatchAsync(string lastChangeVectorSentInThisBatch)
        {
            throw new NotSupportedException();
        }

        public override Task AcknowledgeBatchAsync(long batchId, string changeVector)
        {
            throw new NotSupportedException();
        }
    }

    public interface IEtagSettable
    {
        public void SetStartEtag(long etag);
    }
}
