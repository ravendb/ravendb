// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Esprima;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Constants = Voron.Global.Constants;
using Exception = System.Exception;
using QueryParser = Raven.Server.Documents.Queries.Parser.QueryParser;

namespace Raven.Server.Documents.TcpHandlers
{
    public enum SubscriptionError
    {
        ConnectionRejected,
        Error
    }

    public class SubscriptionOperationScope
    {
        public const string ConnectionPending = "ConnectionPending";
        public const string ConnectionActive = "ConnectionActive";
        public const string BatchSendDocuments = "BatchSendDocuments";
        public const string BatchWaitForAcknowledge = "BatchWaitForAcknowledge";
    }

    public class SubscriptionConnection : SubscriptionConnectionBase
    {
        private static readonly StringSegment DataSegment = new StringSegment("Data");
        private static readonly StringSegment IncludesSegment = new StringSegment(nameof(QueryResult.Includes));
        private static readonly StringSegment CounterIncludesSegment = new StringSegment(nameof(QueryResult.CounterIncludes));
        private static readonly StringSegment TimeSeriesIncludesSegment = new StringSegment(nameof(QueryResult.TimeSeriesIncludes));
        private static readonly StringSegment IncludedCounterNamesSegment = new StringSegment(nameof(QueryResult.IncludedCounterNames));
        private static readonly StringSegment ExceptionSegment = new StringSegment("Exception");
        private static readonly StringSegment TypeSegment = new StringSegment("Type");
        private static readonly TimeSpan InitialConnectionTimeout = TimeSpan.FromMilliseconds(16);

        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly DocumentDatabase _database;

        public long CurrentBatchId;

        public string LastSentChangeVectorInThisConnection;

        public SubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer bufferToCopy, string database)
            : base(tcpConnection, serverStore, bufferToCopy, tcpConnectionDisposable, database, tcpConnection.DocumentDatabase.DatabaseShutdown)
        {
            _database = tcpConnection.DocumentDatabase;
            CurrentBatchId = NonExistentBatch;
        }

        internal static async Task FlushBufferToNetworkAsync(MemoryStream buffer, TcpConnectionOptions tcpConnection, CancellationToken token = default)
        {
            buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await tcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count, token);
            await tcpConnection.Stream.FlushAsync(token);
            tcpConnection.RegisterBytesSent(bytes.Count);
            buffer.SetLength(0);
        }

        protected SubscriptionConnectionsState _subscriptionConnectionsState;

        public SubscriptionConnectionsState GetSubscriptionConnectionState()
        {
            var subscriptions = TcpConnection.DocumentDatabase.SubscriptionStorage.Subscriptions;
            _subscriptionConnectionsState =  subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsState(_database.Name, subId, TcpConnection.DocumentDatabase.SubscriptionStorage));
            return _subscriptionConnectionsState;
        }

        private SubscriptionProcessor _processor;

        public override async Task ProcessSubscriptionAsync()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to process subscription"));
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting processing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            using (_processor = SubscriptionProcessor.Create(this))
            {
                var replyFromClientTask = GetReplyFromClientAsync();

                _processor.AddScript(SetupFilterAndProjectionScript());

                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    _buffer.SetLength(0);

                    var inProgressBatchStats = Stats.CreateInProgressBatchStats();

                    using (var batchScope = inProgressBatchStats.CreateScope())
                    {
                        try
                        {
                            using (TcpConnection.DocumentDatabase.DatabaseInUse(false))
                            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
                            {
                                var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                                var anyDocumentsSentInCurrentIteration =
                                    await TrySendingBatchToClient(docsContext, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats);

                                if (anyDocumentsSentInCurrentIteration == false)
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info($"Did not find any documents to send for subscription {Options.SubscriptionName}");
                                    }

                                    AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                                        $"Acknowledging docs processing progress without sending any documents to client. CV: {_subscriptionConnectionsState.LastChangeVectorSent ?? "None"}"));

                                    Stats.UpdateBatchPerformanceStats(0, false);

                                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                        await SendHeartBeatAsync("Didn't find any documents to send and more then 1000ms passed");

                                    using (docsContext.OpenReadTransaction())
                                    {
                                        var globalEtag = _processor.GetLastItemEtag(docsContext, Subscription.Collection);
                                        if (globalEtag > _subscriptionConnectionsState.GetLastEtagSent())
                                        {
                                            _subscriptionConnectionsState.NotifyHasMoreDocs();
                                        }
                                    }

                                    AssertCloseWhenNoDocsLeft();

                                    if (await WaitForChangedDocsAsync(replyFromClientTask))
                                        continue;
                                }
                            }

                            using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                            {
                                replyFromClientTask = await WaitForClientAck(replyFromClientTask);
                            }

                            var last = Stats.UpdateBatchPerformanceStats(batchScope.GetBatchSize());
                            TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForBatchEnded(_options.SubscriptionName, last);

                        }
                        catch (Exception e)
                        {
                            batchScope.RecordException(e.ToString());
                            throw;
                        }
                    }
                }

                CancellationTokenSource.Token.ThrowIfCancellationRequested();
            }
        }

        private void AssertCloseWhenNoDocsLeft()
        {
            if (_options.CloseWhenNoDocsLeft)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Closing subscription {Options.SubscriptionName} because did not find any documents to send and it's in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode");
                }

                throw new SubscriptionClosedException($"Closing subscription {Options.SubscriptionName} because there were no documents left and client connected in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode");
            }
        }

        /// <summary>
        /// Iterates on a batch in document collection, process it and send documents if found any match
        /// </summary>
        /// <param name="docsContext"></param>
        /// <param name="sendingCurrentBatchStopwatch"></param>
        /// <returns>Whether succeeded finding any documents to send</returns>
        private async Task<bool> TrySendingBatchToClient(DocumentsOperationContext docsContext, Stopwatch sendingCurrentBatchStopwatch,
            SubscriptionBatchStatsScope batchScope, SubscriptionBatchStatsAggregator batchStatsAggregator)
        {
            if (await _subscriptionConnectionsState.WaitForSubscriptionActiveLock(300) == false)
            {
                return false;
            }

            try
            {
                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start trying to send docs to client"));
                bool anyDocumentsSentInCurrentIteration = false;

                using (batchScope.For(SubscriptionOperationScope.BatchSendDocuments))
                {
                    batchScope.RecordBatchInfo(_subscriptionConnectionsState.SubscriptionId, _subscriptionConnectionsState.SubscriptionName, Stats.ConnectionStatsIdForConnection,
                        batchStatsAggregator.Id);

                    int docsToFlush = 0;
                    string lastChangeVectorSentInThisBatch = null;

                    await using (var writer = new AsyncBlittableJsonTextWriter(docsContext, _buffer))
                    {
                        using (TcpConnection.DocumentDatabase.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
                        using (clusterOperationContext.OpenReadTransaction())
                        using (docsContext.OpenReadTransaction())
                        {
                            IncludeDocumentsCommand includeDocumentsCommand = null;
                            IncludeCountersCommand includeCountersCommand = null;
                            IncludeTimeSeriesCommand includeTimeSeriesCommand = null;

                            if (_supportedFeatures.Subscription.Includes)
                                includeDocumentsCommand = new IncludeDocumentsCommand(TcpConnection.DocumentDatabase.DocumentsStorage, docsContext, Subscription.Includes,
                                    isProjection: string.IsNullOrWhiteSpace(Subscription.Script) == false);
                            if (_supportedFeatures.Subscription.CounterIncludes && Subscription.CounterIncludes != null)
                                includeCountersCommand = new IncludeCountersCommand(TcpConnection.DocumentDatabase, docsContext, Subscription.CounterIncludes);
                            if (_supportedFeatures.Subscription.TimeSeriesIncludes && Subscription.TimeSeriesIncludes != null)
                                includeTimeSeriesCommand = new IncludeTimeSeriesCommand(docsContext, Subscription.TimeSeriesIncludes.TimeSeries);

                            CancellationTokenSource.Token.ThrowIfCancellationRequested();

                            _processor.InitializeForNewBatch(clusterOperationContext, docsContext, includeDocumentsCommand);

                            foreach (var result in _processor.GetBatch())
                            {
                                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                                lastChangeVectorSentInThisBatch = ChangeVectorUtils.MergeVectors(
                                    lastChangeVectorSentInThisBatch,
                                    ChangeVectorUtils.NewChangeVector(_database, result.Doc.Etag),
                                    result.Doc.ChangeVector); //merge with this node's local etag

                                if (result.Doc.Data == null)
                                {
                                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                    {
                                        await SendHeartBeatAsync("Skipping docs for more than 1000ms without sending any data");
                                        sendingCurrentBatchStopwatch.Restart();
                                    }

                                    continue;
                                }
                                anyDocumentsSentInCurrentIteration = true;
                                result.Doc.EnsureMetadata();
                                BlittableJsonReaderObject metadata = null;
                                string exceptionString = null;
                                if (result.Exception != null)
                                {
                                    exceptionString = result.Exception.ToString();
                                    if (result.Doc.Data.Modifications != null)
                                    {
                                        using (var old = result.Doc.Data)
                                        {
                                            result.Doc.Data = docsContext.ReadObject(result.Doc.Data, "subsDocAfterModifications");
                                        }
                                    }

                                    result.Doc.Data.TryGet(Client.Constants.Documents.Metadata.Key, out metadata);
                                }

                                WriteDocumentOrException(docsContext, writer, result.Doc, rawDocument: null, metadata, exceptionString, result.Doc.Id, includeDocumentsCommand, includeCountersCommand, includeTimeSeriesCommand);

                                docsToFlush++;
                                batchScope.RecordDocumentInfo(result.Doc.Data.Size);

                                TcpConnection._lastEtagSent = -1;
                                // perform flush for current batch after 1000ms of running or 1 MB
                                if (await FlushBatchIfNeededAsync(sendingCurrentBatchStopwatch, SubscriptionId, writer, _buffer, TcpConnection, Stats.Metrics, _logger, docsToFlush, CancellationTokenSource.Token))
                                {
                                    docsToFlush = 0;
                                    sendingCurrentBatchStopwatch.Restart();
                         
                                }
                            }

                            if (anyDocumentsSentInCurrentIteration)
                            {
                                if (includeDocumentsCommand != null && includeDocumentsCommand.HasIncludesIds())
                                {
                                    var includes = new List<Document>();
                                    includeDocumentsCommand.Fill(includes, includeMissingAsNull: false);
                                    writer.WriteStartObject();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TypeSegment));
                                    writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(IncludesSegment));
                                    writer.WriteComma();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(IncludesSegment));

                                    var (count, sizeInBytes) = await writer.WriteIncludesAsync(docsContext, includes);

                                    batchScope.RecordIncludedDocumentsInfo(count, sizeInBytes);

                                    writer.WriteEndObject();
                                }

                                if (includeCountersCommand != null)
                                {
                                    writer.WriteStartObject();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TypeSegment));
                                    writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(CounterIncludesSegment));
                                    writer.WriteComma();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(CounterIncludesSegment));
                                    writer.WriteCounters(includeCountersCommand.Results);
                                    writer.WriteComma();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(IncludedCounterNamesSegment));
                                    writer.WriteIncludedCounterNames(includeCountersCommand.CountersToGetByDocId);

                                    var size = includeCountersCommand.CountersToGetByDocId.Sum(kvp =>
                                                   kvp.Key.Length + kvp.Value.Sum(name => name.Length)) //CountersToGetByDocId
                                        + includeCountersCommand.Results.Sum(kvp =>
                                            kvp.Value.Sum(counter => counter == null ? 0 : counter.CounterName.Length
                                                                                             + counter.DocumentId.Length
                                                                                             + sizeof(long) //Etag
                                                                                             + sizeof(long) //Total Value
                                       ));
                                    batchScope.RecordIncludedCountersInfo(includeCountersCommand.Results.Sum(x => x.Value.Count), size);

                                    writer.WriteEndObject();
                                }

                                if (includeTimeSeriesCommand != null)
                                {
                                    writer.WriteStartObject();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TypeSegment));
                                    writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(TimeSeriesIncludesSegment));
                                    writer.WriteComma();

                                    writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TimeSeriesIncludesSegment));
                                    var size = await writer.WriteTimeSeriesAsync(includeTimeSeriesCommand.Results, CancellationTokenSource.Token);

                                    batchScope.RecordIncludedTimeSeriesInfo(includeTimeSeriesCommand.Results.Sum(x =>
                                    x.Value.Sum(y => y.Value.Sum(z => z.Entries.Length))), size);

                                    writer.WriteEndObject();
                                }

                                WriteEndOfBatch(writer);

                                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Flushing sent docs to client"));

                                await FlushDocsToClientAsync(SubscriptionId, writer, _buffer, TcpConnection, Stats.Metrics, _logger, docsToFlush, true, CancellationTokenSource.Token);
                            }

                            if (lastChangeVectorSentInThisBatch != null)
                            {
                                //Entire unsent batch could contain docs that have to be skipped, but we still want to update the etag in the cv
                                LastSentChangeVectorInThisConnection = lastChangeVectorSentInThisBatch;
                                CurrentBatchId = await _processor.RecordBatch(lastChangeVectorSentInThisBatch);

                                _subscriptionConnectionsState.LastChangeVectorSent = ChangeVectorUtils.MergeVectors(_subscriptionConnectionsState.LastChangeVectorSent, lastChangeVectorSentInThisBatch);
                                _subscriptionConnectionsState.PreviouslyRecordedChangeVector = ChangeVectorUtils.MergeVectors(_subscriptionConnectionsState.PreviouslyRecordedChangeVector, lastChangeVectorSentInThisBatch);
                            }
                        }
                    }
                }
                return anyDocumentsSentInCurrentIteration;
            }
            finally
            {
                _subscriptionConnectionsState.ReleaseSubscriptionActiveLock();
            }
        }

        internal static void WriteDocumentOrException(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, Document document, BlittableJsonReaderObject rawDocument, BlittableJsonReaderObject metadata, string exception, string id,
            IncludeDocumentsCommand includeDocumentsCommand, IncludeCountersCommand includeCountersCommand, IncludeTimeSeriesCommand includeTimeSeriesCommand)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(TypeSegment));
            writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(DataSegment));
            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(DataSegment));

            if (string.IsNullOrEmpty(exception) == false)
            {
                writer.WriteValue(BlittableJsonToken.StartObject,
                    context.ReadObject(new DynamicJsonValue { [Client.Constants.Documents.Metadata.Key] = metadata }, id)
                );
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ExceptionSegment));
                writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(exception));
            }
            else
            {
                if (rawDocument == null && document == null)
                    throw new InvalidOperationException($"Could not write document to subscription batch, because both {nameof(rawDocument)} and {nameof(document)} are null.");

                if (rawDocument == null)
                {
                    includeDocumentsCommand?.Gather(document);
                    includeCountersCommand?.Fill(document);
                    includeTimeSeriesCommand?.Fill(document);
                    writer.WriteDocument(context, document, metadataOnly: false);
                }
                else
                {
                    writer.WriteValue(BlittableJsonToken.StartObject, rawDocument);
                }

            }
            writer.WriteEndObject();
        }

        internal static void WriteEndOfBatch(AsyncBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(SubscriptionConnectionServerMessage.Type));
            writer.WriteString(nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch));
            writer.WriteEndObject();
        }
        internal static async Task<bool> FlushBatchIfNeededAsync(Stopwatch sendingCurrentBatchStopwatch, long subscriptionId, AsyncBlittableJsonTextWriter writer, MemoryStream buffer, TcpConnectionOptions tcpConnection, SubscriptionConnectionMetrics metrics, Logger logger, int docsToFlush, CancellationToken token = default)
        {
            // perform flush for current batch after 1000ms of running or 1 MB
            if (buffer.Length < Constants.Size.Megabyte && sendingCurrentBatchStopwatch.ElapsedMilliseconds < 1000)
                return false;

            await FlushDocsToClientAsync(subscriptionId, writer, buffer, tcpConnection, metrics, logger, docsToFlush, endOfBatch: false, token: token);
            if (logger.IsInfoEnabled)
            {
                logger.Info($"Finished flushing a batch with {docsToFlush} documents for subscription {subscriptionId}");
            }
            return true;
        }
        internal static async Task FlushDocsToClientAsync(long subscriptionId, AsyncBlittableJsonTextWriter writer, MemoryStream buffer, TcpConnectionOptions tcpConnection, SubscriptionConnectionMetrics metrics, Logger logger, int flushedDocs, bool endOfBatch = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (logger != null && logger.IsInfoEnabled)
                logger.Info($"Flushing {flushedDocs} documents for subscription {subscriptionId} sending to {tcpConnection.TcpClient.Client.RemoteEndPoint} {(endOfBatch ? ", ending batch" : string.Empty)}");

            await writer.FlushAsync(token);
            var bufferSize = buffer.Length;
            await FlushBufferToNetworkAsync(buffer, tcpConnection, token);

            tcpConnection.RegisterBytesSent(bufferSize);

            if (metrics != null)
            {
                metrics.LastMessageSentAt = DateTime.UtcNow;
                metrics.DocsRate?.Mark(flushedDocs);
                metrics.BytesRate?.Mark(bufferSize);
            }
        }

        private async Task<bool> WaitForChangedDocsAsync(Task pendingReply)
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start waiting for changed documents"));
            do
            {
                var hasMoreDocsTask = _subscriptionConnectionsState.WaitForMoreDocs();

                var resultingTask = await Task
                    .WhenAny(hasMoreDocsTask, pendingReply, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(WaitForChangedDocumentsTimeoutInMs))).ConfigureAwait(false);
              
                TcpConnection.DocumentDatabase.ForTestingPurposes?.Subscription_ActionToCallDuringWaitForChangedDocuments?.Invoke();

                if (CancellationTokenSource.IsCancellationRequested)
                    return false;

                if (resultingTask == pendingReply)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                {
                    _subscriptionConnectionsState.NotifyNoMoreDocs();
                    return true;
                }

                await SendHeartBeatAsync("Waiting for changed documents");
                await SendNoopAckAsync();
            } while (CancellationTokenSource.IsCancellationRequested == false);
            return false;
        }

        private SubscriptionPatchDocument SetupFilterAndProjectionScript()
        {
            SubscriptionPatchDocument patch = null;

            if (string.IsNullOrWhiteSpace(Subscription.Script) == false)
            {
                patch = new SubscriptionPatchDocument(Subscription.Script, Subscription.Functions);
            }
            return patch;
        }

        public override void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            Stats.LastConnectionStats.Complete();
            TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForConnectionEnded(this);

            base.Dispose();
        }

        public struct ParsedSubscription
        {
            public string Collection;
            public string Script;
            public string[] Functions;
            public bool Revisions;
            public string[] Includes;
            public string[] CounterIncludes;
            internal TimeSeriesIncludesField TimeSeriesIncludes;
        }

        public static ParsedSubscription ParseSubscriptionQuery(string query)
        {
            var queryParser = new QueryParser();
            queryParser.Init(query);
            var q = queryParser.Parse();

            if (q.IsDistinct)
                throw new NotSupportedException("Subscription does not support distinct queries");
            if (q.From.Index)
                throw new NotSupportedException("Subscription must specify a collection to use");
            if (q.GroupBy != null)
                throw new NotSupportedException("Subscription cannot specify a group by clause");
            if (q.OrderBy != null)
                throw new NotSupportedException("Subscription cannot specify an order by clause");
            if (q.UpdateBody != null)
                throw new NotSupportedException("Subscription cannot specify an update clause");

            bool revisions = false;
            if (q.From.Filter is Queries.AST.BinaryExpression filter)
            {
                switch (filter.Operator)
                {
                    case OperatorType.Equal:
                    case OperatorType.NotEqual:
                        if (!(filter.Left is FieldExpression fe) || fe.Compound.Count != 1)
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        if (string.Equals(fe.Compound[0].Value, "Revisions", StringComparison.OrdinalIgnoreCase) == false)
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        if (filter.Right is ValueExpression ve)
                        {
                            revisions = filter.Operator == OperatorType.Equal && ve.Value == ValueTokenType.True;
                            if (ve.Value != ValueTokenType.True && ve.Value != ValueTokenType.False)
                                throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        }
                        else
                        {
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        }
                        break;

                    default:
                        throw new NotSupportedException("Subscription must not specify a collection filter (move it to the where clause)");
                }
            }
            else if (q.From.Filter != null)
            {
                throw new NotSupportedException("Subscription must not specify a collection filter (move it to the where clause)");
            }

            List<string> includes = null;
            List<string> counterIncludes = null;
            TimeSeriesIncludesField timeSeriesIncludes = null;
            if (q.Include != null)
            {
                foreach (var include in q.Include)
                {
                    switch (include)
                    {
                        case MethodExpression me:
                            var includeType = QueryMethod.GetMethodType(me.Name.Value);
                            switch (includeType)
                            {
                                case MethodType.Counters:
                                    QueryValidator.ValidateIncludeCounter(me.Arguments, q.QueryText, null);

                                    if (counterIncludes == null)
                                        counterIncludes = new List<string>();

                                    if (me.Arguments.Count > 0)
                                    {
                                        var argument = me.Arguments[0];

                                        counterIncludes.Add(ExtractPathFromExpression(argument, q));
                                    }
                                    break;
                                case MethodType.TimeSeries:
                                    QueryValidator.ValidateIncludeTimeseries(me.Arguments, q.QueryText, null);

                                    if (timeSeriesIncludes == null)
                                        timeSeriesIncludes = new TimeSeriesIncludesField();

                                    switch (me.Arguments.Count)
                                    {
                                        case 1:
                                            {
                                                if (!(me.Arguments[0] is MethodExpression methodExpression))
                                                    throw new InvalidQueryException($"Expected to get include '{nameof(MethodType.TimeSeries)}' clause expression, but got: '{me.Arguments[0]}'.", q.QueryText);

                                                switch (methodExpression.Arguments.Count)
                                                {
                                                    case 1:
                                                        {
                                                            // include timeseries(last(11))
                                                            var (type, count) = TimeseriesIncludesHelper.ParseCount(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(Client.Constants.TimeSeries.All, type, count);
                                                            break;
                                                        }
                                                    case 2:
                                                        {
                                                            // include timeseries(last(600, 'seconds'))
                                                            var (type, time) = TimeseriesIncludesHelper.ParseTime(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(Client.Constants.TimeSeries.All, type, time);

                                                            break;
                                                        }
                                                    default:
                                                        throw new InvalidQueryException($"Got invalid arguments count '{methodExpression.Arguments.Count}' in '{methodExpression.Name}' method.", q.QueryText);
                                                }
                                            }
                                            break;
                                        case 2: // include timeseries('Name', last(7, 'months'));
                                            {
                                                if (!(me.Arguments[1] is MethodExpression methodExpression))
                                                    throw new InvalidQueryException($"Expected to get include {nameof(MethodType.TimeSeries)} clause expression, but got: {me.Arguments[1]}.", q.QueryText);

                                                string name = TimeseriesIncludesHelper.ExtractValueFromExpression(me.Arguments[0]);

                                                switch (methodExpression.Arguments.Count)
                                                {
                                                    case 1:
                                                        {
                                                            // last count query
                                                            var (type, count) = TimeseriesIncludesHelper.ParseCount(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(name, type, count);
                                                            break;
                                                        }
                                                    case 2:
                                                        {
                                                            // last time query
                                                            var (type, time) = TimeseriesIncludesHelper.ParseTime(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(name, type, time);
                                                            break;
                                                        }
                                                    default:
                                                        throw new InvalidQueryException($"Got invalid arguments count '{methodExpression.Arguments.Count}' in '{methodExpression.Name}' method.", q.QueryText);
                                                }
                                            }
                                            break;
                                        default:
                                            throw new NotSupportedException($"Invalid number of arguments '{me.Arguments.Count}' in include {nameof(MethodType.TimeSeries)} clause expression.");
                                    }
                                    break;
                                default:
                                    throw new NotSupportedException($"Subscription include expected to get {MethodType.Counters} or {nameof(MethodType.TimeSeries)} but got {includeType}");
                            }
                            break;
                        default:
                            if (includes == null)
                                includes = new List<string>();

                            includes.Add(ExtractPathFromExpression(include, q));
                            break;
                    }
                }

                static string ExtractPathFromExpression(QueryExpression expression, Query q)
                {
                    switch (expression)
                    {
                        case FieldExpression fe:
                            (string fieldPath, string _) = QueryMetadata.ParseExpressionPath(expression, fe.FieldValue, q.From.Alias);
                            return fieldPath;

                        case ValueExpression ve:
                            (string memberPath, string _) = QueryMetadata.ParseExpressionPath(expression, ve.Token.Value, q.From.Alias);
                            return memberPath;

                        default:
                            throw new InvalidOperationException("Subscription only support include of fields, but got: " + expression);
                    }
                }
            }

            var collectionName = q.From.From.FieldValue;
            if (q.Where == null && q.Select == null && q.SelectFunctionBody.FunctionText == null)
            {
                return new ParsedSubscription
                {
                    Collection = collectionName,
                    Revisions = revisions,
                    Includes = includes?.ToArray(),
                    CounterIncludes = counterIncludes?.ToArray(),
                    TimeSeriesIncludes = timeSeriesIncludes
                };
            }

            var writer = new StringWriter();

            if (q.From.Alias != null)
            {
                writer.Write("var ");
                writer.Write(q.From.Alias);
                writer.WriteLine(" = this;");
            }
            else if (q.Select != null || q.SelectFunctionBody.FunctionText != null || q.Load != null)
            {
                throw new InvalidOperationException("Cannot specify a select or load clauses without an alias on the query");
            }
            if (q.Load != null)
            {
                Debug.Assert(q.From.Alias != null);

                var fromAlias = q.From.Alias.Value;
                foreach (var tuple in q.Load)
                {
                    writer.Write("var ");
                    writer.Write(tuple.Alias);
                    writer.Write(" = loadPath(this,'");
                    var fieldExpression = ((FieldExpression)tuple.Expression);
                    if (fieldExpression.Compound[0] != fromAlias)
                        throw new InvalidOperationException("Load clause can only load paths starting from the from alias: " + fromAlias);
                    writer.Write(fieldExpression.FieldValueWithoutAlias);
                    writer.WriteLine("');");
                }
            }
            if (q.Where != null)
            {
                writer.Write("if (");
                new JavascriptCodeQueryVisitor(writer.GetStringBuilder(), q).VisitExpression(q.Where);
                writer.WriteLine(" )");
                writer.WriteLine("{");
            }

            if (q.SelectFunctionBody.FunctionText != null)
            {
                writer.Write(" return ");
                writer.Write(q.SelectFunctionBody.FunctionText);
                writer.WriteLine(";");
            }
            else if (q.Select != null)
            {
                if (q.Select.Count != 1 || q.Select[0].Expression is MethodExpression == false)
                    throw new NotSupportedException("Subscription select clause must specify an object literal");
                writer.WriteLine();
                writer.Write(" return ");
                new JavascriptCodeQueryVisitor(writer.GetStringBuilder(), q).VisitExpression(q.Select[0].Expression);
                writer.WriteLine(";");
            }
            else
            {
                writer.WriteLine(" return true;");
            }
            writer.WriteLine();

            if (q.Where != null)
                writer.WriteLine("}");

            var script = writer.GetStringBuilder().ToString();

            // verify that the JS code parses
            try
            {
                new JavaScriptParser(script).ParseScript();
            }
            catch (Exception e)
            {
                throw new InvalidDataException("Unable to parse: " + script, e);
            }
            return new ParsedSubscription
            {
                Collection = collectionName,
                Revisions = revisions,
                Script = script,
                Functions = q.DeclaredFunctions?.Values?.Select(x => x.FunctionText).ToArray() ?? Array.Empty<string>(),
                Includes = includes?.ToArray(),
                CounterIncludes = counterIncludes?.ToArray()
            };
        }

        public override async Task ReportExceptionAsync(SubscriptionError error, Exception e)
        {
            Stats.ConnectionScope.RecordException(error, e.ToString());
            if (ConnectionException == null && e is SubscriptionException se)
            {
                ConnectionException = se;
            }

            try
            {
                await LogExceptionAndReportToClientAsync(ConnectionException ?? e);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        protected override async Task OnClientAckAsync()
        {
            await _processor.AcknowledgeBatch(CurrentBatchId);

            Stats.Metrics.LastAckReceivedAt = TcpConnection.DocumentDatabase.Time.GetUtcNow();
            Stats.Metrics.AckRate?.Mark();
            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
            });
        }

        public override Task SendNoopAckAsync()
        {
            return _subscriptionConnectionsState.AcknowledgeBatchProcessed(nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                NonExistentBatch, docsToResend: null);
        }

        protected virtual StatusMessageDetails GetDefault()
        {
            return new StatusMessageDetails
            {
                DatabaseName = $"for database '{DatabaseName}'",
                ClientType = "'client worker'",
                SubscriptionType = "subscription"
            };
        }

        protected override StatusMessageDetails GetStatusMessageDetails()
        {
            var message = GetDefault();
            message.DatabaseName = $"{message.DatabaseName} on '{_serverStore.NodeTag}'";
            message.ClientType = $"{message.ClientType} with IP '{TcpConnection.TcpClient.Client.RemoteEndPoint}'";
            message.SubscriptionType = $"{message.SubscriptionType} '{_options?.SubscriptionName}', id '{SubscriptionId}'";

            return message;
        }
    }

    public class SubscriptionConnectionsDetails
    {
        public List<SubscriptionConnectionDetails> Results;
        public string SubscriptionMode;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Results)] = new DynamicJsonArray(Results.Select(d => d.ToJson())),
                [nameof(SubscriptionMode)] = SubscriptionMode
            };
        }
    }

    public class SubscriptionConnectionDetails
    {
        public string ClientUri { get; set; }
        public string WorkerId { get; set; }
        public SubscriptionOpeningStrategy? Strategy { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ClientUri)] = ClientUri,
                [nameof(WorkerId)] = WorkerId,
                [nameof(Strategy)] = Strategy
            };
        }
    }
}
