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

        protected override (IncludeDocumentsCommand IncludesCommand, ITimeSeriesIncludes TimeSeriesIncludes, ICounterIncludes CounterIncludes) CreateIncludeCommands()
        {
            IncludeDocumentsCommand includeDocuments = null;
            ITimeSeriesIncludes includeTimeSeries = null;
            ICounterIncludes includeCounters = null;

            if (_subscription.Includes != null)
                includeDocuments = new IncludeDocumentsCommand(Database.DocumentsStorage, DocsContext, _subscription.Includes,
                    isProjection: string.IsNullOrWhiteSpace(_subscription.Script) == false);

            if (_subscription.CounterIncludes != null)
                includeCounters = new IncludeCountersCommand(Database, DocsContext, _subscription.CounterIncludes);

            if (_subscription.TimeSeriesIncludes?.TimeSeries != null)
                includeTimeSeries = new IncludeTimeSeriesCommand(DocsContext, _subscription.TimeSeriesIncludes.TimeSeries);

            return (includeDocuments, includeTimeSeries, includeCounters);
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

        protected override (IncludeDocumentsCommand IncludesCommand, ITimeSeriesIncludes TimeSeriesIncludes, ICounterIncludes CounterIncludes) CreateIncludeCommands()
        {
            IncludeDocumentsCommand includeDocuments = null;
            ITimeSeriesIncludes includeTimeSeries = null;
            ICounterIncludes includeCounters = null;

            if (_subscription.Includes != null)
                includeDocuments = new IncludeDocumentsCommand(Database.DocumentsStorage, DocsContext, _subscription.Includes,
                    isProjection: string.IsNullOrWhiteSpace(_subscription.Script) == false);

            if (_subscription.CounterIncludes != null)
                includeCounters = new IncludeCountersCommand(Database, DocsContext, _subscription.CounterIncludes);

            if (_subscription.TimeSeriesIncludes?.TimeSeries != null)
                includeTimeSeries = new IncludeTimeSeriesCommand(DocsContext, _subscription.TimeSeriesIncludes.TimeSeries);

            return (includeDocuments, includeTimeSeries, includeCounters);
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
