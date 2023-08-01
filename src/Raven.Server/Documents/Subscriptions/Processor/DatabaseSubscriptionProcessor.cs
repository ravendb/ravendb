using System;
using System.Collections.Generic;
using System.Diagnostics;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Documents.Subscriptions.Processor
{
    public abstract class DatabaseSubscriptionProcessor<T> : DatabaseSubscriptionProcessorBase<T>
    {
        protected SubscriptionFetcher<T> Fetcher;
        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out DatabaseIncludesCommandImpl includesCommands)
        {
            var release = base.InitializeForNewBatch(clusterContext, out includesCommands);

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

        protected override ConflictStatus GetConflictStatus(string changeVector)
        {
            var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                remoteAsString: changeVector,
                localAsString: SubscriptionState.ChangeVectorForNextBatchStartingPoint);
            return conflictStatus;
        }

        protected override bool CanContinueBatch(SubscriptionBatchItemStatus batchItemStatus, SubscriptionBatchStatsScope batchScope, int numberOfDocs, Stopwatch sendingCurrentBatchStopwatch)
        {
            var size = batchScope?.GetBatchSize() ?? 0L;
            if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes) >= MaximumAllowedMemory)
                return false;
            if (numberOfDocs >= BatchSize)
                return false;

            return base.CanContinueBatch(batchItemStatus, batchScope, numberOfDocs, sendingCurrentBatchStopwatch);
        }

        protected override string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, SubscriptionBatchItem batchItem)
        {
            if (batchItem.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend) // got this document from resend
                return currentLast;

            return ChangeVectorUtils.MergeVectors(
                currentLast,
                ChangeVectorUtils.NewChangeVector(Database, batchItem.Document.Etag, context),
                batchItem.Document.ChangeVector);
            //merge with this node's local etag
        }

        protected override SubscriptionBatchItem GetBatchItem(T item)
        {
            var batchItem = ShouldSend(item, out var reason);

            if (batchItem.Status == SubscriptionBatchItemStatus.Send)
            {
                if (IncludesCmd != null && IncludesCmd.IncludeDocumentsCommand != null && Run != null)
                    IncludesCmd.IncludeDocumentsCommand.AddRange(Run.Includes, batchItem.Document.Id);

                if (batchItem.Document.Data != null)
                    Fetcher.MarkDocumentSent();

                return batchItem;
            }

            if (Logger.IsInfoEnabled)
                Logger.Info(reason, batchItem.Exception);

            if (batchItem.Status  == SubscriptionBatchItemStatus.Exception)
            {
                if (batchItem.Document.Data != null)
                    Fetcher.MarkDocumentSent();

                return batchItem;
            }

            return batchItem;
        }

        protected abstract SubscriptionFetcher<T> CreateFetcher();

        protected abstract SubscriptionBatchItem ShouldSend(T item, out string reason);
    }

    public abstract class DatabaseSubscriptionProcessorBase<TItem> : AbstractSubscriptionProcessor<DatabaseIncludesCommandImpl, TItem>, IDatabaseSubscriptionProcessor
    {
        protected readonly long MaximumAllowedMemory;

        protected readonly DocumentDatabase Database;
        protected DocumentsOperationContext DocsContext;
        protected SubscriptionConnectionsState SubscriptionConnectionsState;
        protected HashSet<long> Active;

        public SubscriptionPatchDocument Patch { get; set; }
        protected ScriptRunner.SingleRun Run;
        private ScriptRunner.ReturnRun? _returnRun;

        protected DatabaseSubscriptionProcessorBase(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) : base(server, connection, database.Name)
        {
            Database = database;
            MaximumAllowedMemory = new Size(Database.Is32Bits ? 4 : 32, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
        }

        public override void InitializeProcessor()
        {
            base.InitializeProcessor();

            SubscriptionConnectionsState = Database.SubscriptionStorage.Subscriptions[Connection.SubscriptionId];
            Active = SubscriptionConnectionsState.GetActiveBatches();
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out DatabaseIncludesCommandImpl includesCommands)
        {
            var release = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocsContext);
            try
            {
                DocsContext.OpenReadTransaction();
                base.InitializeForNewBatch(clusterContext, out includesCommands);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }

        protected override DatabaseIncludesCommandImpl CreateIncludeCommands()
        {
            var includes = CreateIncludeCommandsInternal(Database, DocsContext, Connection, Connection.Subscription);
            return includes;
        }

        internal static DatabaseIncludesCommandImpl CreateIncludeCommandsInternal(DocumentDatabase database, DocumentsOperationContext context,
            ISubscriptionConnection connection, SubscriptionConnection.ParsedSubscription subscription)
        {
            var hasIncludes = TryCreateIncludesCommand(database, context, connection, subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries);

            var includes = hasIncludes ? new DatabaseIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters) : null;
            return includes;
        }

        public static bool TryCreateIncludesCommand(DocumentDatabase database, DocumentsOperationContext context, ISubscriptionConnection connection,
            SubscriptionConnection.ParsedSubscription subscription, out IncludeCountersCommand includeCounters, out IncludeDocumentsCommand includeDocuments, out IncludeTimeSeriesCommand includeTimeSeries)
        {
            includeTimeSeries = null;
            includeCounters = null;
            includeDocuments = null;

            bool hasIncludes = false;
            if (connection == null)
            {
                if (subscription.Includes != null)
                {
                    // test subscription with includes
                    includeDocuments = new IncludeDocumentsCommand(database.DocumentsStorage, context, subscription.Includes,
                        isProjection: string.IsNullOrWhiteSpace(subscription.Script) == false);
                    hasIncludes = true;
                }
            }
            else if (connection.SupportedFeatures.Subscription.Includes)
            {
                includeDocuments = new IncludeDocumentsCommand(database.DocumentsStorage, context, subscription.Includes,
                    isProjection: string.IsNullOrWhiteSpace(subscription.Script) == false);
                hasIncludes = true;
            }

            if (subscription.CounterIncludes != null)
            {
                if (connection != null && connection.SupportedFeatures.Subscription.CounterIncludes)
                {
                    includeCounters = new IncludeCountersCommand(database, context, subscription.CounterIncludes);
                    hasIncludes = true;
                }
                else
                {
                    includeCounters = new IncludeCountersCommand(database, context, subscription.CounterIncludes);
                    hasIncludes = true;
                }
            }

            if (subscription.TimeSeriesIncludes != null)
            {
                if (connection != null && connection.SupportedFeatures.Subscription.TimeSeriesIncludes)
                {
                    includeTimeSeries = new IncludeTimeSeriesCommand(context, subscription.TimeSeriesIncludes.TimeSeries);
                    hasIncludes = true;
                }
                else
                {
                    includeTimeSeries = new IncludeTimeSeriesCommand(context, subscription.TimeSeriesIncludes.TimeSeries);
                    hasIncludes = true;
                }
            }

            return hasIncludes;
        }

        protected void InitializeScript()
        {
            if (Patch == null)
                return;

            if (_returnRun != null)
                return; // already init

            _returnRun = Database.Scripts.GetScriptRunner(Patch, true, out Run);
        }

        private protected sealed class ProjectionMetadataModifier : JsBlittableBridge.IResultModifier
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
