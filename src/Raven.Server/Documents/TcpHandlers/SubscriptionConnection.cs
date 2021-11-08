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
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using Constants = Voron.Global.Constants;
using Exception = System.Exception;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection : SubscriptionConnectionBase, IDisposable
    {
        private const int WaitForChangedDocumentsTimeoutInMs = 3000;
        private static int _batchStatsId;
        internal static readonly StringSegment DataSegment = new StringSegment("Data");
        internal static readonly StringSegment IncludesSegment = new StringSegment(nameof(QueryResult.Includes));
        internal static readonly StringSegment CounterIncludesSegment = new StringSegment(nameof(QueryResult.CounterIncludes));
        internal static readonly StringSegment TimeSeriesIncludesSegment = new StringSegment(nameof(QueryResult.TimeSeriesIncludes));
        internal static readonly StringSegment IncludedCounterNamesSegment = new StringSegment(nameof(QueryResult.IncludedCounterNames));
        internal static readonly StringSegment ExceptionSegment = new StringSegment("Exception");
        internal static readonly StringSegment TypeSegment = new StringSegment("Type");

        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly AsyncManualResetEvent _waitForMoreDocuments;
        private readonly TcpConnectionHeaderMessage.SupportedFeatures _supportedFeatures;

        private string _lastChangeVector;
        private long _startEtag;
        private SubscriptionPatchDocument _filterAndProjectionScript;
        private SubscriptionDocumentsFetcher _documentsFetcher;
        
        public ParsedSubscription Subscription;

        public SubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer bufferToCopy) 
            : base(tcpConnection, serverStore, bufferToCopy, tcpConnectionDisposable, 
                tcpConnection.ShardedContext == null ? tcpConnection.DocumentDatabase.Name : tcpConnection.ShardedContext.DatabaseName, 
                CancellationTokenSource.CreateLinkedTokenSource(tcpConnection.DocumentDatabase.DatabaseShutdown))
        {
            _supportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Subscription, tcpConnection.ProtocolVersion);
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
        }

        public override SubscriptionConnectionState OpenSubscription()
        {
            return TcpConnection.DocumentDatabase.SubscriptionStorage.OpenSubscription(this);
        }

        private async Task InitAsync()
        {
            if (TcpConnection.ShardedContext == null)
            {
                Shard = null;
            }
            else
            {
                Shard = new ShardData { ShardName = TcpConnection.DocumentDatabase.Name, DatabaseId = TcpConnection.DocumentDatabase.DbBase64Id };
            }

            await ParseSubscriptionOptionsAsync();
            var dbNameStr = Shard == null ? $"for database '{Database}'" : $"for shard '{Shard.ShardName}' of database '{Database}'";
            var message = $"A connection for subscription ID {SubscriptionId}, {dbNameStr} was received from remote IP {TcpConnection.TcpClient.Client.RemoteEndPoint}";
            AddToStatusDescription(message);
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(message);
            }

            // first, validate details and make sure subscription exists
            SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, Database, CancellationTokenSource.Token);
            Subscription = ParseSubscriptionQuery(SubscriptionState.Query);

            if (_supportedFeatures.Subscription.Includes == false)
            {
                if (Subscription.Includes != null && Subscription.Includes.Length > 0)
                    throw new SubscriptionInvalidStateException($"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Includes.");
            }

            if (_supportedFeatures.Subscription.CounterIncludes == false)
            {
                if (Subscription.CounterIncludes != null && Subscription.CounterIncludes.Length > 0)
                    throw new SubscriptionInvalidStateException($"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Counter Includes.");
            }

            if (_supportedFeatures.Subscription.TimeSeriesIncludes == false)
            {
                if (Subscription.TimeSeriesIncludes != null && Subscription.TimeSeriesIncludes.TimeSeries.Count > 0)
                    throw new SubscriptionInvalidStateException($"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support TimeSeries Includes.");
            }

            await TryConnectSubscription();

            try
            {
                _activeConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionActive);

                // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)
                SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, Database, CancellationTokenSource.Token);

                Subscription = ParseSubscriptionQuery(SubscriptionState.Query);

                await SendNoopAck();
                await WriteJsonAsync(new DynamicJsonValue
                {
                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                    [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                }, TcpConnection);

                await TcpConnection.DocumentDatabase.SubscriptionStorage.UpdateClientConnectionTime(SubscriptionState.SubscriptionId,
                    SubscriptionState.SubscriptionName, Database, Shard, SubscriptionState.MentorNode);
            }
            catch
            {
                DisposeOnDisconnect.Dispose();
                throw;
            }
        }

        public static void SendSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var remoteEndPoint = tcpConnectionOptions.TcpClient.Client.RemoteEndPoint;

            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress("Subscription");
            try
            {
                var connection = new SubscriptionConnection(server, tcpConnectionOptions, tcpConnectionDisposable, buffer);
                try
                {
                    Task.Run(async () =>
                    {
                        using (tcpConnectionOptions)
                        using (tcpConnectionDisposable)
                        using (connection)
                        {
                            connection.CreateStatsScope();

                            try
                            {
                                bool gotSemaphore;
                                if ((gotSemaphore = tcpConnectionOptions.DocumentDatabase.SubscriptionStorage.TryEnterSemaphore()) == false)
                                {
                                    throw new SubscriptionClosedException(
                                        $"Cannot open new subscription connection, max amount of concurrent connections reached ({tcpConnectionOptions.DocumentDatabase.Configuration.Subscriptions.MaxNumberOfConcurrentConnections})");
                                }

                                try
                                {
                                    await connection.InitAsync();
                                    await connection.ProcessSubscriptionAsync();
                                }
                                catch (SubscriptionInvalidStateException)
                                {
                                    connection._pendingConnectionScope.Dispose();
                                    throw;
                                }
                                finally
                                {
                                    if (gotSemaphore)
                                    {
                                        tcpConnectionOptions.DocumentDatabase.SubscriptionStorage.ReleaseSubscriptionsSemaphore();
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                await connection.RecordExceptionAndReportToClient(connection.ConnectionException ?? e, sharded: false);
                            }
                            finally
                            {
                                var dbNameStr = connection.Shard == null ? $"for database '{connection.Database}'" : $"for shard '{connection.Shard.ShardName}' of database '{connection.Database}'";
                                var msg = $"Finished processing subscription '{connection.SubscriptionId}', {dbNameStr}  / from client '{remoteEndPoint}'.";
                                connection.AddToStatusDescription(msg);
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info(msg);
                                }
                            }
                        }
                    });
                }
                catch (Exception)
                {
                    connection?.Dispose();
                    throw;
                }
            }
            catch (Exception)
            {
                tcpConnectionDisposable?.Dispose();

                throw;
            }
        }

        private IDisposable RegisterForNotificationOnNewDocuments()
        {
            void RegisterNotification(DocumentChange notification)
            {
                if (Client.Constants.Documents.Collections.AllDocumentsCollection.Equals(Subscription.Collection, StringComparison.OrdinalIgnoreCase) ||
                    notification.CollectionName.Equals(Subscription.Collection, StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        _waitForMoreDocuments.Set();
                    }
                    catch
                    {
                        if (CancellationTokenSource.IsCancellationRequested)
                            return;
                        try
                        {
                            CancellationTokenSource.Cancel();
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }
            }

            TcpConnection.DocumentDatabase.Changes.OnDocumentChange += RegisterNotification;
            return new DisposableAction(
                    () =>
                    {
                        TcpConnection.DocumentDatabase.Changes.OnDocumentChange -= RegisterNotification;
                    });
        }

        private async Task ProcessSubscriptionAsync()
        {
            AddToStatusDescription("Starting to process subscription");
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting processing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            using (DisposeOnDisconnect)
            using (RegisterForNotificationOnNewDocuments())
            {
                var replyFromClientTask = GetReplyFromClientAsync();

                string subscriptionChangeVectorBeforeCurrentBatch = SubscriptionState.ChangeVectorForNextBatchStartingPoint;
                _startEtag = GetStartEtagForSubscription(SubscriptionState);
                _filterAndProjectionScript = SetupFilterAndProjectionScript();
                var useRevisions = Subscription.Revisions;
                _documentsFetcher = new SubscriptionDocumentsFetcher(TcpConnection.DocumentDatabase, _options.MaxDocsPerBatch, SubscriptionId, TcpConnection.TcpClient.Client.RemoteEndPoint, Subscription.Collection, useRevisions, SubscriptionState, _filterAndProjectionScript);

                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    _buffer.SetLength(0);

                    var inProgressBatchStats = _lastBatchStats = new SubscriptionBatchStatsAggregator(Interlocked.Increment(ref _batchStatsId), _lastBatchStats);

                    using (var batchScope = inProgressBatchStats.CreateScope())
                    {
                        try
                        {
                            using (TcpConnection.DocumentDatabase.DatabaseInUse(false))
                            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
                            {
                                var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                                var anyDocumentsSentInCurrentIteration = await TrySendingBatchToClient(docsContext, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats.Id);

                                if (anyDocumentsSentInCurrentIteration == false)
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info($"Did not find any documents to send for subscription {Options.SubscriptionName}");
                                    }
                                    AddToStatusDescription($"Acknowledging docs processing progress without sending any documents to client. CV: {_lastChangeVector}");

                                    await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(Database, Shard, SubscriptionId,
                                        Options.SubscriptionName,
                                        // if this is a new subscription that we sent anything in this iteration,
                                        // _lastChangeVector is null, so let's not change it
                                        _lastChangeVector ??
                                        nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                                        subscriptionChangeVectorBeforeCurrentBatch);

                                    subscriptionChangeVectorBeforeCurrentBatch = _lastChangeVector ?? SubscriptionState.ChangeVectorForNextBatchStartingPoint;

                                    UpdateBatchPerformanceStats(0, false);

                                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                        await SendHeartBeat("Didn't find any documents to send and more then 1000ms passed");

                                    using (docsContext.OpenReadTransaction())
                                    {
                                        var globalEtag = useRevisions
                                            ? TcpConnection.DocumentDatabase.DocumentsStorage.RevisionsStorage.GetLastRevisionEtag(docsContext, Subscription.Collection)
                                            : TcpConnection.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(docsContext.Transaction.InnerTransaction, Subscription.Collection);

                                        if (globalEtag > _startEtag)
                                            continue;
                                    }

                                    AssertCloseWhenNoDocsLeft();

                                    if (await WaitForChangedDocuments(replyFromClientTask, docsContext))
                                        continue;
                                }
                            }

                            using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                            {
                                (replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch) =
                                    await WaitForClientAck(replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch, sharded: false);
                            }

                            UpdateBatchPerformanceStats(batchScope.GetBatchSize());
                        }
                        catch (Exception e)
                        {
                            batchScope.RecordException(e.Message);
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
            SubscriptionBatchStatsScope batchScope, int subscriptionBatchStatsId)
        {
            AddToStatusDescription("Start trying to send docs to client");
            bool anyDocumentsSentInCurrentIteration = false;

            using (batchScope.For(SubscriptionOperationScope.BatchSendDocuments))
            {
                batchScope.RecordBatchInfo(_connectionState.Connection.SubscriptionId, _connectionState.SubscriptionName, _connectionState.Connection._connectionStatsIdForConnection, subscriptionBatchStatsId);

                int docsToFlush = 0;

                await using (var writer = new AsyncBlittableJsonTextWriter(docsContext, _buffer))
                {
                    using (docsContext.OpenReadTransaction())
                    {
                        IncludeDocumentsCommand includeDocumentsCommand = null;
                        IncludeCountersCommand includeCountersCommand = null;
                        IncludeTimeSeriesCommand includeTimeSeriesCommand = null;

                        if (_supportedFeatures.Subscription.Includes && Subscription.Includes != null)
                            includeDocumentsCommand = new IncludeDocumentsCommand(TcpConnection.DocumentDatabase.DocumentsStorage, docsContext, Subscription.Includes, isProjection: _filterAndProjectionScript != null);
                        if (_supportedFeatures.Subscription.CounterIncludes && Subscription.CounterIncludes != null)
                            includeCountersCommand = new IncludeCountersCommand(TcpConnection.DocumentDatabase, docsContext, Subscription.CounterIncludes);
                        if (_supportedFeatures.Subscription.TimeSeriesIncludes && Subscription.TimeSeriesIncludes != null)
                            includeTimeSeriesCommand = new IncludeTimeSeriesCommand(docsContext, Subscription.TimeSeriesIncludes.TimeSeries);

                        foreach (var result in _documentsFetcher.GetDataToSend(docsContext, includeDocumentsCommand, _startEtag))
                        {
                            CancellationTokenSource.Token.ThrowIfCancellationRequested();

                            _startEtag = result.Doc.Etag;
                            _lastChangeVector = string.IsNullOrEmpty(SubscriptionState.ChangeVectorForNextBatchStartingPoint)
                                ? result.Doc.ChangeVector
                                : ChangeVectorUtils.MergeVectors(result.Doc.ChangeVector, SubscriptionState.ChangeVectorForNextBatchStartingPoint);
                            if (Shard != null)
                                Shard.LocalChangeVector = result.Doc.ChangeVector;
                            if (result.Doc.Data == null)
                            {
                                if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                {
                                    await SendHeartBeat("Skipping docs for more than 1000ms without sending any data");
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
                            if (await FlushBatchIfNeeded(sendingCurrentBatchStopwatch, SubscriptionId, writer, _buffer, TcpConnection, Stats, _logger, docsToFlush, CancellationTokenSource.Token))
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
                                includeDocumentsCommand.Fill(includes);
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

                                batchScope.RecordIncludedCountersInfo(includeCountersCommand.Results.Sum(x => x.Value.Count));

                                writer.WriteEndObject();
                            }

                            if (includeTimeSeriesCommand != null)
                            {
                                writer.WriteStartObject();

                                writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TypeSegment));
                                writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(TimeSeriesIncludesSegment));
                                writer.WriteComma();

                                writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TimeSeriesIncludesSegment));
                                writer.WriteTimeSeries(includeTimeSeriesCommand.Results);

                                batchScope.RecordIncludedTimeSeriesInfo(includeTimeSeriesCommand.Results.Sum(x =>
                                    x.Value.Sum(y => y.Value.Sum(z => z.Entries.Length))));

                                writer.WriteEndObject();
                            }

                            WriteEndOfBatch(writer);

                            AddToStatusDescription("Flushing sent docs to client");

                            await FlushDocsToClient(SubscriptionId, writer, _buffer, TcpConnection, Stats, _logger, docsToFlush, true, CancellationTokenSource.Token);
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"Finished sending a batch with {docsToFlush} documents for subscription {Options.SubscriptionName}");
                            }
                        }
                    }
                }
            }

            return anyDocumentsSentInCurrentIteration;
        }

        internal static void WriteEndOfBatch(AsyncBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(SubscriptionConnectionServerMessage.Type));
            writer.WriteString(nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch));
            writer.WriteEndObject();
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

        private long GetStartEtagForSubscription(SubscriptionState subscription)
        {
            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
            using (docsContext.OpenReadTransaction())
            {
                long startEtag = 0;

                if (string.IsNullOrEmpty(subscription.ChangeVectorForNextBatchStartingPoint))
                    return startEtag;

                var changeVector = subscription.ChangeVectorForNextBatchStartingPoint.ToChangeVector();
                var cv = changeVector.FirstOrDefault(x => x.DbId == TcpConnection.DocumentDatabase.DbBase64Id);
                if (cv.DbId == "" && cv.Etag == 0 && cv.NodeTag == 0)
                    return startEtag;
                return cv.Etag;
            }
        }

        private async Task TryFlushEmptyBatchToClient(JsonOperationContext docsContext)
        {
            if (Shard == null)
                return;

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Flushing empty batch for sharded subscription '{SubscriptionId}' sending to '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(docsContext, _buffer))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(SubscriptionConnectionServerMessage.Type));
                writer.WriteString(nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch));
                writer.WriteEndObject();
                await writer.FlushAsync();
            }
            var bufferSize = _buffer.Length;
            await FlushBufferToNetwork(_buffer, TcpConnection, CancellationTokenSource.Token);
            TcpConnection.RegisterBytesSent(bufferSize);
        }

        internal static async Task<bool> FlushBatchIfNeeded(Stopwatch sendingCurrentBatchStopwatch, long subscriptionId, AsyncBlittableJsonTextWriter writer, MemoryStream buffer, TcpConnectionOptions tcpConnection, SubscriptionConnectionStats stats, Logger logger, int docsToFlush, CancellationToken token = default)
        {
            // perform flush for current batch after 1000ms of running or 1 MB
            if (buffer.Length < Constants.Size.Megabyte && sendingCurrentBatchStopwatch.ElapsedMilliseconds < 1000) 
                return false;

            await FlushDocsToClient(subscriptionId, writer, buffer, tcpConnection, stats, logger, docsToFlush, endOfBatch: false, token: token);
            return true;
        }

        internal static async Task FlushDocsToClient(long subscriptionId, AsyncBlittableJsonTextWriter writer, MemoryStream buffer, TcpConnectionOptions tcpConnection, SubscriptionConnectionStats stats, Logger logger, int flushedDocs, bool endOfBatch = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (logger != null && logger.IsInfoEnabled)
                logger.Info($"Flushing {flushedDocs} documents for subscription {subscriptionId} sending to {tcpConnection.TcpClient.Client.RemoteEndPoint} {(endOfBatch ? ", ending batch" : string.Empty)}");

            await writer.FlushAsync(token);
            var bufferSize = buffer.Length;
            await FlushBufferToNetwork(buffer, tcpConnection, token);

            tcpConnection.RegisterBytesSent(bufferSize);

            if (stats != null)
            {
                stats.LastMessageSentAt = DateTime.UtcNow;
                stats.DocsRate?.Mark(flushedDocs);
                stats.BytesRate?.Mark(bufferSize);
            }
        }

        internal static async Task FlushBufferToNetwork(MemoryStream buffer, TcpConnectionOptions tcpConnection, CancellationToken token = default)
        {
            buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await tcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count, token);
            await tcpConnection.Stream.FlushAsync(token);
            tcpConnection.RegisterBytesSent(bytes.Count);
            buffer.SetLength(0);
        }

        private async Task<bool> WaitForChangedDocuments(Task pendingReply, JsonOperationContext docsContext)
        {
            AddToStatusDescription("Start waiting for changed documents");
            do
            {
                var hasMoreDocsTask = _waitForMoreDocuments.WaitAsync();
                var resultingTask = await Task.WhenAny(hasMoreDocsTask, pendingReply, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(WaitForChangedDocumentsTimeoutInMs))).ConfigureAwait(false);

                if (CancellationTokenSource.IsCancellationRequested)
                    return false;
                if (resultingTask == pendingReply)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                {
                    _waitForMoreDocuments.Reset();
                    return true;
                }

                await SendHeartBeat("Waiting for changed documents");
                await SendNoopAck();
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

        internal override async Task<string> OnClientAck(string subscriptionChangeVectorBeforeCurrentBatch)
        {
            await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(Database, Shard,
                SubscriptionId,
                Options.SubscriptionName,
                _lastChangeVector,
                subscriptionChangeVectorBeforeCurrentBatch);
            subscriptionChangeVectorBeforeCurrentBatch = _lastChangeVector;
            Stats.LastAckReceivedAt = TcpConnection.DocumentDatabase.Time.GetUtcNow();
            Stats.AckRate?.Mark();

            await WriteJsonAsync(new DynamicJsonValue { [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm) }, TcpConnection);

            return subscriptionChangeVectorBeforeCurrentBatch;
        }

        public new void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;

            base.Dispose();

            try
            {
                _waitForMoreDocuments.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }

            TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForConnectionEnded(this);

            //TODO: egor
           // GC.SuppressFinalize(this);
        }
    }

    public class SubscriptionConnectionDetails
    {
        public string ClientUri { get; set; }
        public SubscriptionOpeningStrategy? Strategy { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ClientUri)] = ClientUri,
                [nameof(Strategy)] = Strategy
            };
        }
    }
}
