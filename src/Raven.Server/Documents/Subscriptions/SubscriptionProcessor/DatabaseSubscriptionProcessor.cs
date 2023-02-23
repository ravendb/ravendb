
using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public abstract class DatabaseSubscriptionProcessor<T> : DatabaseSubscriptionProcessor
    {
        protected SubscriptionFetcher<T> Fetcher;

        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out IncludeDocumentsCommand includesCommands, out ITimeSeriesIncludes timeSeriesIncludes,
            out ICounterIncludes counterIncludes)
        {
            var release = base.InitializeForNewBatch(clusterContext, out includesCommands, out timeSeriesIncludes, out counterIncludes);

            try
            {
                Fetcher = CreateFetcher();
                Fetcher.Initialize(clusterContext, DocsContext, Active);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }

        protected (Document Doc, Exception Exception) GetBatchItem(T item)
        {
            if (ShouldSend(item, out var reason, out var exception, out var result))
            {
                if (IncludesCmd != null && Run != null)
                    IncludesCmd.AddRange(Run.Includes, result.Id);

                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, null);
            }

            if (Logger.IsInfoEnabled)
                Logger.Info(reason, exception);

            if (exception != null)
            {
                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, exception);
            }

            result.Data = null;
            return (result, null);
        }

        protected abstract SubscriptionFetcher<T> CreateFetcher();

        protected abstract bool ShouldSend(T item, out string reason, out Exception exception, out Document result);
    }

    public abstract class DatabaseSubscriptionProcessor : AbstractSubscriptionProcessor<IncludeDocumentsCommand>
    {
        protected readonly Size MaximumAllowedMemory;

        protected readonly DocumentDatabase Database;
        protected DocumentsOperationContext DocsContext;
        protected SubscriptionConnectionsState SubscriptionConnectionsState;
        protected HashSet<long> Active;

        public SubscriptionPatchDocument Patch;
        protected ScriptRunner.SingleRun Run;
        private ScriptRunner.ReturnRun? _returnRun;

        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) : base(server, connection, database.Name)
        {
            Database = database;
            MaximumAllowedMemory = new Size(Database.Is32Bits ? 4 : 32, SizeUnit.Megabytes);
        }

        public override void InitializeProcessor()
        {
            base.InitializeProcessor();

            SubscriptionConnectionsState = Database.SubscriptionStorage.Subscriptions[Connection.SubscriptionId];
            Active = SubscriptionConnectionsState.GetActiveBatches();
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out IncludeDocumentsCommand includesCommands, out ITimeSeriesIncludes timeSeriesIncludes,
            out ICounterIncludes counterIncludes)
        {
            var release = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocsContext);
            try
            {
                DocsContext.OpenReadTransaction();
                base.InitializeForNewBatch(clusterContext, out includesCommands, out timeSeriesIncludes, out counterIncludes);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }

        protected override (IncludeDocumentsCommand IncludesCommand, ITimeSeriesIncludes TimeSeriesIncludes, ICounterIncludes CounterIncludes) CreateIncludeCommands()
        {
            IncludeDocumentsCommand includeDocuments = null;
            ITimeSeriesIncludes includeTimeSeries = null;
            ICounterIncludes includeCounters = null;

            if (Connection.SupportedFeatures.Subscription.Includes)
                includeDocuments = new IncludeDocumentsCommand(Database.DocumentsStorage, DocsContext, Connection.Subscription.Includes,
                    isProjection: string.IsNullOrWhiteSpace(Connection.Subscription.Script) == false);
            if (Connection.SupportedFeatures.Subscription.CounterIncludes && Connection.Subscription.CounterIncludes != null)
                includeCounters = new IncludeCountersCommand(Database, DocsContext, Connection.Subscription.CounterIncludes);
            if (Connection.SupportedFeatures.Subscription.TimeSeriesIncludes && Connection.Subscription.TimeSeriesIncludes != null)
                includeTimeSeries = new IncludeTimeSeriesCommand(DocsContext, Connection.Subscription.TimeSeriesIncludes.TimeSeries);

            return (includeDocuments, includeTimeSeries, includeCounters);
        }

        protected void InitializeScript()
        {
            if (Patch == null)
                return;

            if (_returnRun != null)
                return; // already init

            _returnRun = Database.Scripts.GetScriptRunner(Patch, true, out Run);
        }

        private protected class ProjectionMetadataModifier : JsBlittableBridge.IResultModifier
        {
            public static readonly ProjectionMetadataModifier Instance = new ProjectionMetadataModifier();

            private ProjectionMetadataModifier()
            {
            }

            public void Modify(ObjectInstance json)
            {
                ObjectInstance metadata;
                var value = json.Get(Constants.Documents.Metadata.Key);
                if (value.Type == Types.Object)
                    metadata = value.AsObject();
                else
                {
                    metadata = new JsObject(json.Engine);
                    json.Set(Constants.Documents.Metadata.Key, metadata, false);
                }

                metadata.Set(Constants.Documents.Metadata.Projection, JsBoolean.True, false);
            }
        }

        public abstract long GetLastItemEtag(DocumentsOperationContext context, string collection);

        public override void Dispose()
        {
            base.Dispose();
            _returnRun?.Dispose();
        }
    }
}
