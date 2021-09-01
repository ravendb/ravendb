// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
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
        private static readonly StringSegment DataSegment = new StringSegment("Data");
        private static readonly StringSegment IncludesSegment = new StringSegment(nameof(QueryResult.Includes));
        private static readonly StringSegment CounterIncludesSegment = new StringSegment(nameof(QueryResult.CounterIncludes));
        private static readonly StringSegment TimeSeriesIncludesSegment = new StringSegment(nameof(QueryResult.TimeSeriesIncludes));
        private static readonly StringSegment IncludedCounterNamesSegment = new StringSegment(nameof(QueryResult.IncludedCounterNames));
        private static readonly StringSegment ExceptionSegment = new StringSegment("Exception");
        private static readonly StringSegment TypeSegment = new StringSegment("Type");

        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly AsyncManualResetEvent _waitForMoreDocuments;
        private readonly TcpConnectionHeaderMessage.SupportedFeatures _supportedFeatures;

        private string _lastChangeVector;
        private long _startEtag;
        private SubscriptionPatchDocument _filterAndProjectionScript;
        private SubscriptionDocumentsFetcher _documentsFetcher;
        private bool _isDisposed;
        
        public ParsedSubscription Subscription;

        public SubscriptionConnection(ServerStore serverStore, TcpConnectionOptions connectionOptions, IDisposable tcpConnectionDisposable,
            JsonOperationContext.MemoryBuffer bufferToCopy) : base(connectionOptions, serverStore, bufferToCopy, tcpConnectionDisposable)
        {
            _supportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Subscription, connectionOptions.ProtocolVersion);
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
        }

        private async Task InitAsync()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                (SubscriptionWorkerOptions subscriptionWorkerOptions, long? id) = await ParseSubscriptionOptionsAsync(context, _serverStore, TcpConnection, _copiedBuffer.Buffer, TcpConnection.DocumentDatabase.Name, CancellationTokenSource.Token);
                _options = subscriptionWorkerOptions;
                if (id.HasValue)
                    SubscriptionId = id.Value;
            }

            var message = $"A connection for subscription ID {SubscriptionId} was received from remote IP {TcpConnection.TcpClient.Client.RemoteEndPoint}";
            AddToStatusDescription(message);
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(message);
            }

            // first, validate details and make sure subscription exists
            SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, CancellationTokenSource.Token);
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
            _pendingConnectionScope.Dispose();

            try
            {
                _activeConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionActive);

                // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)
                SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, CancellationTokenSource.Token);

                Subscription = ParseSubscriptionQuery(SubscriptionState.Query);

                await SendNoopAck(TcpConnection, SubscriptionId, Options.SubscriptionName);
                await WriteJsonAsync(new DynamicJsonValue
                {
                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                    [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                }, TcpConnection);

                await TcpConnection.DocumentDatabase.SubscriptionStorage.UpdateClientConnectionTime(SubscriptionState.SubscriptionId,
                    SubscriptionState.SubscriptionName, SubscriptionState.MentorNode);
            }
            catch
            {
                DisposeOnDisconnect.Dispose();
                throw;
            }
        }

        private async Task FlushBufferToNetwork()
        {
            _buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await TcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);
            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(bytes.Count);
            _buffer.SetLength(0);
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
                            connection._lastConnectionStats = new SubscriptionConnectionStatsAggregator(_connectionStatsId, null);
                            connection._connectionScope = connection._lastConnectionStats.CreateScope();

                            connection._pendingConnectionScope = connection._connectionScope.For(SubscriptionOperationScope.ConnectionPending);

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
                                if (e is SubscriptionInUseException)
                                    connection._connectionScope.RecordException(SubscriptionError.ConnectionRejected, e.Message);
                                else
                                    connection._connectionScope.RecordException(SubscriptionError.Error, e.Message);

                                var errorMessage = $"Failed to process subscription {connection.SubscriptionId} / from client {remoteEndPoint}";
                                connection.AddToStatusDescription($"{errorMessage}. Sending response to client");
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info(errorMessage, e);
                                }

                                try
                                {
                                    await ReportExceptionToClient(server, tcpConnectionOptions, connection, connection.ConnectionException ?? e, connection._logger);
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }
                            finally
                            {
                                connection.AddToStatusDescription("Finished processing subscription");
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info(
                                        $"Finished processing subscription {connection.SubscriptionId} / from client {remoteEndPoint}");
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
                var replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);

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

                                var anyDocumentsSentInCurrentIteration = await TrySendingBatchToClient(docsContext, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats);

                                if (anyDocumentsSentInCurrentIteration == false)
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info($"Did not find any documents to send for subscription {Options.SubscriptionName}");
                                    }
                                    AddToStatusDescription($"Acknowledging docs processing progress without sending any documents to client. CV: {_lastChangeVector}");

                                    await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(SubscriptionId,
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

                                    if (await WaitForChangedDocuments(replyFromClientTask))
                                        continue;
                                }
                            }

                            using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                            {
                                (replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch) =
                                    await WaitForClientAck(replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch);
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

        private async Task<(Task<SubscriptionConnectionClientMessage> ReplyFromClientTask, string SubscriptionChangeVectorBeforeCurrentBatch)>
            WaitForClientAck(Task<SubscriptionConnectionClientMessage> replyFromClientTask, string subscriptionChangeVectorBeforeCurrentBatch)
        {
            SubscriptionConnectionClientMessage clientReply;
            while (true)
            {
                var result = await Task.WhenAny(replyFromClientTask,
                    TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(5000), CancellationTokenSource.Token)).ConfigureAwait(false);
                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                if (result == replyFromClientTask)
                {
                    clientReply = await replyFromClientTask;
                    if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                    {
                        CancellationTokenSource.Cancel();
                        break;
                    }

                    replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);
                    break;
                }
                
                await SendHeartBeat("Waiting for client ACK");
                await SendNoopAck(TcpConnection, SubscriptionId, Options.SubscriptionName);
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                        SubscriptionId,
                        Options.SubscriptionName,
                        _lastChangeVector,
                        subscriptionChangeVectorBeforeCurrentBatch);
                    subscriptionChangeVectorBeforeCurrentBatch = _lastChangeVector;
                    Stats.LastAckReceivedAt = TcpConnection.DocumentDatabase.Time.GetUtcNow();
                    Stats.AckRate?.Mark();
                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
                    }, TcpConnection);

                    break;

                //precaution, should not reach this case...
                case SubscriptionConnectionClientMessage.MessageType.DisposedNotification:
                    CancellationTokenSource.Cancel();
                    break;

                default:
                    throw new ArgumentException("Unknown message type from client " +
                                                clientReply.Type);
            }

            return (replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch);
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
            AddToStatusDescription("Start trying to send docs to client");
            bool anyDocumentsSentInCurrentIteration = false;

            using (batchScope.For(SubscriptionOperationScope.BatchSendDocuments))
            {
                batchScope.RecordBatchInfo(_connectionState.Connection.SubscriptionId, _connectionState.SubscriptionName, _connectionState.Connection._connectionStatsIdForConnection, batchStatsAggregator.Id);

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
                            writer.WriteStartObject();

                            writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TypeSegment));
                            writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(DataSegment));
                            writer.WriteComma();
                            writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(DataSegment));
                            result.Doc.EnsureMetadata();

                            if (result.Exception != null)
                            {
                                if (result.Doc.Data.Modifications != null)
                                {
                                    result.Doc.Data = docsContext.ReadObject(result.Doc.Data, "subsDocAfterModifications");
                                }

                                var metadata = result.Doc.Data[Client.Constants.Documents.Metadata.Key];
                                writer.WriteValue(BlittableJsonToken.StartObject,
                                    docsContext.ReadObject(new DynamicJsonValue
                                    {
                                        [Client.Constants.Documents.Metadata.Key] = metadata
                                    }, result.Doc.Id)
                                );
                                writer.WriteComma();
                                writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(ExceptionSegment));
                                writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(result.Exception.ToString()));
                            }
                            else
                            {
                                includeDocumentsCommand?.Gather(result.Doc);
                                includeCountersCommand?.Fill(result.Doc);
                                includeTimeSeriesCommand?.Fill(result.Doc);

                                writer.WriteDocument(docsContext, result.Doc, metadataOnly: false);
                            }

                            writer.WriteEndObject();
                            docsToFlush++;
                            batchScope.RecordDocumentInfo(result.Doc.Data.Size);

                            TcpConnection._lastEtagSent = -1;
                            // perform flush for current batch after 1000ms of running or 1 MB
                            if (_buffer.Length > Constants.Size.Megabyte ||
                                sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                            {
                                await FlushDocsToClient(writer, docsToFlush);
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

                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(SubscriptionConnectionServerMessage.Type));
                            writer.WriteString(nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch));
                            writer.WriteEndObject();

                            AddToStatusDescription("Flushing sent docs to client");
                            await FlushDocsToClient(writer, docsToFlush, true);
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

        private long GetStartEtagForSubscription(SubscriptionState subscription)
        {
            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
            using (docsContext.OpenReadTransaction())
            {
                long startEtag = 0;

                if (string.IsNullOrEmpty(subscription.ChangeVectorForNextBatchStartingPoint))
                    return startEtag;

                long etag;
                var changeVector = subscription.ChangeVectorForNextBatchStartingPoint.ToChangeVector();
                if (ShardHelper.IsShardedName(TcpConnection.DocumentDatabase.Name))
                {
                    // check if cv is part of the shards
                    var dbName = ShardHelper.ToDatabaseName(TcpConnection.DocumentDatabase.Name);
                    var result = TcpConnection.DocumentDatabase.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(dbName);
                    if (result.DatabaseStatus == DatabasesLandlord.DatabaseSearchResult.Status.Sharded)
                    {
                        for (int i = 0; i < result.ShardedContext.Count; i++)
                        {
                            var db = TcpConnection.DocumentDatabase.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(result.ShardedContext.GetShardedDatabaseName(i)).DatabaseTask.Result;
                            foreach (var part in changeVector)
                            {
                                if (part.DbId == db.DbBase64Id && part.Etag != 0)
                                {
                                    return part.Etag;
                                }
                            }
                        }
                    }
                }

                var cv = changeVector.FirstOrDefault(x => x.DbId == TcpConnection.DocumentDatabase.DbBase64Id);
                etag = cv.Etag;
                if (cv.DbId == "" && cv.Etag == 0 && cv.NodeTag == 0)
                    return startEtag;

                return etag;
            }
        }



        private async Task FlushDocsToClient(AsyncBlittableJsonTextWriter writer, int flushedDocs, bool endOfBatch = false)
        {
            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Flushing {flushedDocs} documents for subscription {SubscriptionId} sending to {TcpConnection.TcpClient.Client.RemoteEndPoint} {(endOfBatch ? ", ending batch" : string.Empty)}");
            }

            await writer.FlushAsync();
            var bufferSize = _buffer.Length;
            await FlushBufferToNetwork();
            Stats.LastMessageSentAt = DateTime.UtcNow;
            Stats.DocsRate?.Mark(flushedDocs);
            Stats.BytesRate?.Mark(bufferSize);
            TcpConnection.RegisterBytesSent(bufferSize);
        }

        private async Task<bool> WaitForChangedDocuments(Task pendingReply)
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
                await SendNoopAck(TcpConnection, SubscriptionId, Options.SubscriptionName);
            } while (CancellationTokenSource.IsCancellationRequested == false);
            return false;
        }

        internal static async Task SendNoopAck(TcpConnectionOptions tcpConnection, long subscriptionId, string subscriptionName)
        {
            await tcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                subscriptionId,
                subscriptionName,
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange));
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
