using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions.Processor;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionConnectionBase<TIncludesCommand> : ISubscriptionConnection
        where TIncludesCommand : AbstractIncludesCommand
    {
        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");
        private static readonly int MaxBufferCapacityInBytes = 2 * Constants.Size.Megabyte;
        private static readonly StringSegment DataSegment = new("Data");
        private static readonly StringSegment ExceptionSegment = new("Exception");

        private readonly AbstractSubscriptionStorage _subscriptions;
        protected readonly ServerStore ServerStore;
        private readonly IDisposable _tcpConnectionDisposable;
        internal readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;

        internal SubscriptionWorkerOptions _options;
        internal readonly Logger _logger;

        public string LastSentChangeVectorInThisConnection { get; set; }
        public readonly ConcurrentQueue<string> RecentSubscriptionStatuses = new();
        public SubscriptionWorkerOptions Options => _options;
        public SubscriptionException ConnectionException;
        public long SubscriptionId { get; set; }
        public readonly string DatabaseName;
        public TcpConnectionOptions TcpConnection { get; }
        public CancellationTokenSource CancellationTokenSource { get; }

        public SubscriptionOpeningStrategy Strategy => _options.Strategy;
        public readonly string ClientUri;
        public long LastModifiedIndex => SubscriptionState.RaftCommandIndex;

        public string WorkerId => _options.WorkerId ??= Guid.NewGuid().ToString();
        public SubscriptionState SubscriptionState { get; set; }
        public SubscriptionConnection.ParsedSubscription Subscription { get; private set; }

        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; }
        public readonly SubscriptionStatsCollector Stats;

        public Task SubscriptionConnectionTask { get; set; }
        private readonly MemoryStream _buffer = new();

        private readonly DisposeOnce<SingleAttempt> _disposeOnce;

        protected ISubscriptionProcessor<TIncludesCommand> Processor;

        private TestingStuff _forTestingPurposes;

        internal Task<SubscriptionConnectionClientMessage> _lastReplyFromClientTask;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            public AsyncManualResetEvent PauseConnection;
        }

        protected SubscriptionConnectionBase(AbstractSubscriptionStorage subscriptions, TcpConnectionOptions tcpConnection, ServerStore serverStore, JsonOperationContext.MemoryBuffer memoryBuffer,
            IDisposable tcpConnectionDisposable, string database, CancellationToken token)
        {
            TcpConnection = tcpConnection;
            _subscriptions = subscriptions;
            ServerStore = serverStore;
            _copiedBuffer = memoryBuffer.Clone(tcpConnection.ContextPool);
            _tcpConnectionDisposable = tcpConnectionDisposable;

            DatabaseName = database;
            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _logger = LoggingSource.Instance.GetLogger(ExtractDatabaseNameForLogging(tcpConnection), GetType().FullName);

            ClientUri = tcpConnection.TcpClient.Client.RemoteEndPoint.ToString();

            SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Subscription, tcpConnection.ProtocolVersion);
            Stats = new SubscriptionStatsCollector();

            _disposeOnce = new DisposeOnce<SingleAttempt>(DisposeInternal);
        }

        private static string ExtractDatabaseNameForLogging(TcpConnectionOptions tcpConnection)
        {
            if (tcpConnection.DocumentDatabase != null)
                return tcpConnection.DocumentDatabase.Name;

            if (tcpConnection.DatabaseContext != null)
                return tcpConnection.DatabaseContext.DatabaseName;

            return null;
        }

        public abstract ISubscriptionProcessor<TIncludesCommand> CreateProcessor(SubscriptionConnectionBase<TIncludesCommand> connection);

        public async Task ProcessSubscriptionAsync<TState, TConnection>(TState state)
            where TState : AbstractSubscriptionConnectionsState<TConnection, TIncludesCommand>
            where TConnection : SubscriptionConnectionBase<TIncludesCommand>
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to process subscription"));
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting processing documents for subscription {SubscriptionId} received from {ClientUri}");
            }

            using (Processor = CreateProcessor(this))
            {
                _lastReplyFromClientTask = GetReplyFromClientAsync();

                AfterProcessorCreation();

                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    _buffer.SetLength(0);

                    if (_forTestingPurposes?.PauseConnection != null)
                        await _forTestingPurposes.PauseConnection.WaitAsync();

                    var inProgressBatchStats = Stats.CreateInProgressBatchStats();

                    using (var batchScope = inProgressBatchStats.CreateScope())
                    {
                        try
                        {
                            using var markInUse = MarkInUse();
                            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                            var status = await TrySendingBatchToClientAsync<TState, TConnection>(state, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats);

                            await HandleBatchStatusAsync<TState, TConnection>(state, status, sendingCurrentBatchStopwatch, markInUse, batchScope);
                        }
                        catch (Exception e)
                        {
                            OnError(e);
                            batchScope.RecordException(e.ToString());
                            throw;
                        }
                    }
                }

                CancellationTokenSource.Token.ThrowIfCancellationRequested();
            }
        }

        internal virtual async Task HandleBatchStatusAsync<TState, TConnection>(
            TState state, SubscriptionBatchStatus status, 
            Stopwatch sendingCurrentBatchStopwatch, 
            SubscriptionConnectionInUse markInUse,
            SubscriptionBatchStatsScope batchScope) where TState : AbstractSubscriptionConnectionsState<TConnection, TIncludesCommand>
            where TConnection : SubscriptionConnectionBase<TIncludesCommand>
        {
            switch (status)
            {
                case SubscriptionBatchStatus.EmptyBatch:
                    await LogBatchStatusAndUpdateStatsAsync(sendingCurrentBatchStopwatch, $"Got '{nameof(SubscriptionBatchStatus.EmptyBatch)}' for subscription '{Options.SubscriptionName}'.");

                    if (FoundAboutMoreDocs())
                        state.NotifyHasMoreDocs();

                    AssertCloseWhenNoDocsLeft();

                    // we might wait for new documents for a long times, lets reduce the stream capacity
                    if (_buffer.Capacity > MaxBufferCapacityInBytes)
                    {
                        Debug.Assert(_buffer.Length <= MaxBufferCapacityInBytes, $"{_buffer.Length} <= {MaxBufferCapacityInBytes}");
                        _buffer.Capacity = MaxBufferCapacityInBytes;
                    }

                    if (await WaitForChangedDocsAsync(state))
                        return;

                    await CancelSubscriptionAndThrowAsync();

                    break;

                case SubscriptionBatchStatus.DocumentsSent:
                    markInUse?.Dispose();

                    using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                    {
                        await WaitForClientAck();
                    }

                    var last = Stats.UpdateBatchPerformanceStats(batchScope.GetBatchSize());
                    RaiseNotificationForBatchEnd(_options.SubscriptionName, last);

                    break;

                default:
                    throw new ArgumentOutOfRangeException($"{status}");
            }
        }

        internal async Task CancelSubscriptionAndThrowAsync()
        {
            //client sent DisposedNotification or cts was canceled
            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            var result = await Task.WhenAny(_lastReplyFromClientTask, TimeoutManager.WaitFor(ISubscriptionConnection.HeartbeatTimeout, CancellationTokenSource.Token));
            CancellationTokenSource.Token.ThrowIfCancellationRequested();
            if (result == _lastReplyFromClientTask)
            {
                var clientReply = await _lastReplyFromClientTask;
                Debug.Assert(clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification,
                    "clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification");

                CancellationTokenSource.Cancel();
            }
            else
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Expected to get '{SubscriptionConnectionClientMessage.MessageType.DisposedNotification}' from client, but there was no reply.");
                }
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        protected async Task LogBatchStatusAndUpdateStatsAsync(Stopwatch sendingCurrentBatchStopwatch, string logMessage)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info(logMessage);

            Stats.UpdateBatchPerformanceStats(0, false);

            if (sendingCurrentBatchStopwatch.Elapsed >= ISubscriptionConnection.HeartbeatTimeout)
                await SendHeartBeatAsync($"Didn't find any documents to send and more then {ISubscriptionConnection.HeartbeatTimeout.TotalMilliseconds}ms passed");

        }

        protected virtual void OnError(Exception e) { }

        /// <summary>
        /// Iterates on a batch in document collection, process it and send documents if found any match
        /// </summary>
        /// <returns>Whether succeeded finding any documents to send</returns>
        private async Task<SubscriptionBatchStatus> TrySendingBatchToClientAsync<TState, TConnection>(TState state, Stopwatch sendingCurrentBatchStopwatch,
            SubscriptionBatchStatsScope batchScope, SubscriptionBatchStatsAggregator batchStatsAggregator)
            where TState : AbstractSubscriptionConnectionsState<TConnection, TIncludesCommand>
            where TConnection : SubscriptionConnectionBase<TIncludesCommand>
        {
            if (await state.WaitForSubscriptionActiveLock(300) == false)
                return SubscriptionBatchStatus.EmptyBatch;

            try
            {
                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start trying to send docs to client"));

                using (batchScope.For(SubscriptionOperationScope.BatchSendDocuments))
                {
                    batchScope.RecordBatchInfo(state.SubscriptionId, state.SubscriptionName,
                        Stats.ConnectionStatsIdForConnection,
                        batchStatsAggregator.Id);

                    using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, _buffer))
                    using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
                    using (clusterOperationContext.OpenReadTransaction())
                    {
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        using (Processor.InitializeForNewBatch(clusterOperationContext, out var includeCommand))
                        {
                            SubscriptionBatchResult result = await Processor.GetBatchAsync(batchScope, sendingCurrentBatchStopwatch);

                            var batchStatus = await TryRecordBatchAndUpdateStatusAsync(clusterOperationContext, result);
                            if (batchStatus != SubscriptionBatchStatus.DocumentsSent)
                            {
                                // empty batch or active migration
                                return batchStatus;
                            }

                            CancellationTokenSource.Token.ThrowIfCancellationRequested();

                            foreach (var item in result.CurrentBatch)
                            {
                                using (item.Document)
                                {
                                    WriteDocument(writer, context, item, includeCommand);
                                }
                            }

                            if (includeCommand != null)
                            {
                                await includeCommand.WriteIncludesAsync(writer, context, batchScope, CancellationTokenSource.Token);
                            }

                            WriteEndOfBatch(writer);

                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Flushing sent docs to client"));

                            await FlushDocsToClientAsync(SubscriptionId, writer, _buffer, TcpConnection, Stats.Metrics, _logger, result.CurrentBatch.Count, endOfBatch: true,
                                CancellationTokenSource.Token);

                            return SubscriptionBatchStatus.DocumentsSent;
                        }
                    }
                }
            }
            finally
            {
                state.ReleaseSubscriptionActiveLock();
            }
        }

        private void WriteDocument(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, SubscriptionBatchItem result,
            TIncludesCommand includeCommand)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ISubscriptionConnection.TypeSegment));
            writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(DataSegment));
            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(DataSegment));
            result.Document.EnsureMetadata();

            if (result.Exception != null)
            {
                if (result.Document.Data.Modifications != null)
                {
                    result.Document.Data = context.ReadObject(result.Document.Data, "subsDocAfterModifications");
                }

                var metadata = result.Document.Data[Client.Constants.Documents.Metadata.Key];
                writer.WriteValue(BlittableJsonToken.StartObject,
                    context.ReadObject(new DynamicJsonValue { [Client.Constants.Documents.Metadata.Key] = metadata }, result.Document.Id)
                );
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ExceptionSegment));
                writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(result.Exception.ToString()));
            }
            else
            {
                GatherIncludesForDocument(includeCommand, result.Document);
                writer.WriteDocument(context, result.Document, metadataOnly: false);
            }

            writer.WriteEndObject();
        }

        protected abstract void GatherIncludesForDocument(TIncludesCommand includeDocuments, Document document);

        public async Task ReportExceptionAsync(SubscriptionError error, Exception e)
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

        public abstract SubscriptionConnectionInfo CreateConnectionInfo();

        protected virtual async Task<bool> WaitForChangedDocsAsync(AbstractSubscriptionConnectionsState state)
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Start waiting for changed documents"));
            do
            {
                var hasMoreDocsTask = state.WaitForMoreDocs();

                var resultingTask = await Task
                    .WhenAny(hasMoreDocsTask, _lastReplyFromClientTask, TimeoutManager.WaitFor(ISubscriptionConnection.HeartbeatTimeout)).ConfigureAwait(false);

                TcpConnection.DocumentDatabase?.ForTestingPurposes?.Subscription_ActionToCallDuringWaitForChangedDocuments?.Invoke();

                if (CancellationTokenSource.IsCancellationRequested)
                    return false;

                if (resultingTask == _lastReplyFromClientTask)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                    return true;

                await SendHeartBeatAsync("Waiting for changed documents");
                await SendNoopAckAsync();

                if (FoundAboutMoreDocs())
                    return true;

            } while (CancellationTokenSource.IsCancellationRequested == false);

            return false;
        }

        internal async Task<SubscriptionConnectionClientMessage> GetReplyFromClientAsync()
        {
            try
            {
                using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var blittable = await context.ParseToMemoryAsync(
                           TcpConnection.Stream,
                           "Reply from subscription client",
                           BlittableJsonDocumentBuilder.UsageMode.None,
                           _copiedBuffer.Buffer))
                {
                    TcpConnection.LastEtagReceived = -1;
                    TcpConnection.RegisterBytesReceived(blittable.Size);
                    return JsonDeserializationServer.SubscriptionConnectionClientMessage(blittable);
                }
            }
            catch (EndOfStreamException e)
            {
                throw new SubscriptionConnectionDownException("No reply from the subscription client.", e);
            }
            catch (IOException)
            {
                if (_disposeOnce.Disposed == false)
                    throw;

                return new SubscriptionConnectionClientMessage { ChangeVector = null, Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification };
            }
            catch (ObjectDisposedException)
            {
                return new SubscriptionConnectionClientMessage { ChangeVector = null, Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification };
            }
        }

        public async Task InitAsync()
        {
            var message = CreateStatusMessage(ConnectionStatus.Create);
            AddToStatusDescription(message);
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(message);
            }

            // first, validate details and make sure subscription exists
            await RefreshAsync();

            AssertSupportedFeatures();
        }

        public async Task RefreshAsync(long? registerConnectionDurationInTicks = null)
        {
            SubscriptionState = await AssertSubscriptionConnectionDetails(registerConnectionDurationInTicks);
            Subscription = SubscriptionConnection.ParseSubscriptionQuery(SubscriptionState.Query);
        }

        public async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long? registerConnectionDurationInTicks) =>
            await AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, registerConnectionDurationInTicks, CancellationTokenSource.Token);

        protected virtual RawDatabaseRecord GetRecord(ClusterOperationContext context) => ServerStore.Cluster.ReadRawDatabaseRecord(context, DatabaseName);

        private async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long id, string name, long? registerConnectionDurationInTicks, CancellationToken token)
        {
            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id, token);

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var record = GetRecord(context))
            {
#pragma warning disable CS0618
                var subscription = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, DatabaseName, name);
#pragma warning restore CS0618

                if (subscription.ArchivedDataProcessingBehavior is null)
                {
                    // from 5.x version
                    subscription.ArchivedDataProcessingBehavior = _subscriptions.GetDefaultArchivedDataProcessingBehavior();  
                }

                var whoseTaskIsIt = _subscriptions.GetSubscriptionResponsibleNode(context, subscription);
                if (whoseTaskIsIt == null && record.DeletionInProgress.ContainsKey(ServerStore.NodeTag))
                    throw new DatabaseDoesNotExistException(
                        $"Stopping subscription '{name}' on node {ServerStore.NodeTag}, because database '{DatabaseName}' is being deleted.");
                
                if (record.IsDisabled)
                    throw new DatabaseDisabledException($"Stopping subscription '{name}' on node {ServerStore.NodeTag}, because database '{DatabaseName}' is disabled.");

                if (whoseTaskIsIt != ServerStore.NodeTag)
                {
                    var databaseTopologyAvailabilityExplanation = new Dictionary<string, string>();

                    string generalState;
                    RachisState currentState = ServerStore.Engine.CurrentState;
                    if (currentState == RachisState.Candidate || currentState == RachisState.Passive)
                    {
                        generalState =
                            $"Current node ({ServerStore.NodeTag}) is in {currentState.ToString()} state therefore, we can't answer who's task is it and returning null";
                    }
                    else
                    {
                        generalState = currentState.ToString();
                    }

                    databaseTopologyAvailabilityExplanation["NodeState"] = generalState;

                    var topology = record.TopologyForSubscriptions();
                    FillNodesAvailabilityReportForState(subscription, topology, databaseTopologyAvailabilityExplanation, stateGroup: topology.Rehabs, stateName: "rehab");
                    FillNodesAvailabilityReportForState(subscription, topology, databaseTopologyAvailabilityExplanation, stateGroup: topology.Promotables,
                        stateName: "promotable");

                    foreach (var member in topology.Members)
                    {
                        if (whoseTaskIsIt != null)
                        {
                            if (whoseTaskIsIt == subscription.MentorNode && member == subscription.MentorNode)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is the mentor node and a valid member of the topology, it should be the mentor node";
                            }
                            else if (whoseTaskIsIt != member)
                            {
                                databaseTopologyAvailabilityExplanation[member] =
                                    "Is a valid member of the topology, but not chosen to be the node running the subscription";
                            }
                            else if (whoseTaskIsIt == member)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is a valid member of the topology and is chosen to be running the subscription";
                            }
                        }
                        else
                        {
                            databaseTopologyAvailabilityExplanation[member] =
                                "Is a valid member of the topology but was not chosen to run the subscription, we didn't find any other match either";
                        }
                    }

                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Subscription with id '{id}' and name '{name}' can't be processed on current node ({ServerStore.NodeTag}), because it belongs to {whoseTaskIsIt}",
                        whoseTaskIsIt,
                        databaseTopologyAvailabilityExplanation, id)
                    { RegisterConnectionDurationInTicks = registerConnectionDurationInTicks };
                }

                if (subscription.Disabled || _subscriptions.DisableSubscriptionTasks)
                    throw new SubscriptionClosedException($"The subscription with id '{id}' and name '{name}' is disabled and cannot be used until enabled");

                return subscription;
            }

            static void FillNodesAvailabilityReportForState(SubscriptionState subscription, DatabaseTopology topology,
                Dictionary<string, string> databaseTopologyAvailabilityExplanation, List<string> stateGroup, string stateName)
            {
                foreach (var nodeInGroup in stateGroup)
                {
                    string rehabMessage = subscription.MentorNode == nodeInGroup
                        ? $"Although this node is a mentor, it's state is {stateName} and can't run the subscription"
                        : $"Node's state is {stateName}, can't run subscription";

                    if (topology.DemotionReasons.TryGetValue(nodeInGroup, out var demotionReason))
                    {
                        rehabMessage = rehabMessage + ". Reason:" + demotionReason;
                    }

                    databaseTopologyAvailabilityExplanation[nodeInGroup] = rehabMessage;
                }
            }
        }

        public void RecordConnectionInfo() => Stats.ConnectionScope.RecordConnectionInfo(SubscriptionState, ClientUri, Options.Strategy, WorkerId);

        protected virtual void AssertSupportedFeatures()
        {
            if (_options.Strategy == SubscriptionOpeningStrategy.Concurrent)
            {
                ServerStore.LicenseManager.AssertCanAddConcurrentDataSubscriptions();
            }

            if (SupportedFeatures.Subscription.Includes == false)
            {
                if (Subscription.Includes is { Length: > 0 })
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Includes.");
            }

            if (SupportedFeatures.Subscription.CounterIncludes == false)
            {
                if (Subscription.CounterIncludes is { Length: > 0 })
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Counter Includes.");
            }

            if (SupportedFeatures.Subscription.TimeSeriesIncludes == false)
            {
                if (Subscription.TimeSeriesIncludes != null && Subscription.TimeSeriesIncludes.TimeSeries.Count > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support TimeSeries Includes.");
            }
        }

        protected virtual void AssertCloseWhenNoDocsLeft()
        {
            if (_options.CloseWhenNoDocsLeft)
            {
                var reason =
                    $"Closing subscription {Options.SubscriptionName} because there were no documents left and client connected in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode";
                if (_logger.IsInfoEnabled)
                    _logger.Info(reason);

                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, reason));
                throw new SubscriptionClosedException(reason, canReconnect: false, noDocsLeft: true);
            }
        }

        protected async Task LogExceptionAndReportToClientAsync(Exception e)
        {
            var errorMessage = CreateStatusMessage(ConnectionStatus.Fail, e.ToString());
            AddToStatusDescription($"{errorMessage}. Sending response to client");
            if (_logger.IsInfoEnabled && e is not OperationCanceledException)
                _logger.Info(errorMessage, e);

            await ReportExceptionToClientAsync(e);
        }

        protected async Task ReportExceptionToClientAsync(Exception ex, int recursionDepth = 0)
        {
            if (recursionDepth == 2)
                return;
            try
            {
                switch (ex)
                {
                    case SubscriptionDoesNotExistException:
                    case DatabaseDoesNotExistException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.NotFound),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionClosedException sce:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Closed),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString(),
                            [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                            {
                                [nameof(SubscriptionClosedException.CanReconnect)] = sce.CanReconnect,
                                [nameof(SubscriptionClosedException.NoDocsLeft)] = sce.NoDocsLeft
                            }
                        });
                        break;
                    case SubscriptionInvalidStateException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionInUseException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.InUse),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionDoesNotBelongToNodeException subscriptionDoesNotBelongException:
                        {
                            if (string.IsNullOrEmpty(subscriptionDoesNotBelongException.AppropriateNode) == false)
                            {
                                try
                                {
                                    using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                                    using (ctx.OpenReadTransaction())
                                    {
                                        // check that the subscription exists on AppropriateNode
                                        var clusterTopology = ServerStore.GetClusterTopology(ctx);
                                        using (var requester = ClusterRequestExecutor.CreateForShortTermUse(
                                                   clusterTopology.GetUrlFromTag(subscriptionDoesNotBelongException.AppropriateNode),
                                                   ServerStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
                                        {
                                            await requester.ExecuteAsync(new WaitForRaftIndexCommand(subscriptionDoesNotBelongException.Index), ctx);
                                        }
                                    }
                                }
                                catch
                                {
                                    // we let the client try to connect to AppropriateNode
                                }
                            }

                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Redirecting subscription client to different server"));
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Subscription does not belong to current node", ex);
                            }

                            await WriteJsonAsync(new DynamicJsonValue
                            {
                                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Redirect),
                                [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                                [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString(),
                                [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                                {
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)] = subscriptionDoesNotBelongException.AppropriateNode,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.CurrentTag)] = ServerStore.NodeTag,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RegisterConnectionDurationInTicks)] =
                                        subscriptionDoesNotBelongException.RegisterConnectionDurationInTicks,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.Reasons)] =
                                        new DynamicJsonArray(subscriptionDoesNotBelongException.Reasons.Select(item => new DynamicJsonValue { [item.Key] = item.Value }))
                                }
                            });
                            break;
                        }
                    case SubscriptionChangeVectorUpdateConcurrencyException:
                        {
                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                                $"Subscription change vector update concurrency error, reporting to '{ClientUri}'"));
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Subscription change vector update concurrency error", ex);
                            }

                            await WriteJsonAsync(new DynamicJsonValue
                            {
                                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.ConcurrencyReconnect),
                                [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                                [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                            });
                            break;
                        }
                    case LicenseLimitException:
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case RachisApplyException { InnerException: SubscriptionException } commandExecution:
                        await ReportExceptionToClientAsync(commandExecution.InnerException, recursionDepth - 1);
                        break;
                    default:

                        if (ex is not OperationCanceledException)
                        {
                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Fail, $"Subscription error on subscription {ex}"));

                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Subscription error", ex);
                            }
                        }

                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Error),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.None),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                }
            }
            catch
            {
                // ignored
            }
        }

        public enum ConnectionStatus
        {
            Create,
            Fail,
            Info
        }

        public sealed class StatusMessageDetails
        {
            public string DatabaseName;
            public string ClientType;
            public string SubscriptionType;
        }

        protected string CreateStatusMessage(ConnectionStatus status, string info = null)
        {
            var message = GetStatusMessageDetails();
            var dbNameStr = message.DatabaseName;
            var clientType = message.ClientType;
            var subsType = message.SubscriptionType;

            string m;
            switch (status)
            {
                case ConnectionStatus.Create:
                    m = $"[CREATE] Received a connection for {subsType}, {dbNameStr} from {clientType}";
                    break;
                case ConnectionStatus.Fail:
                    m = $"[FAIL] for {subsType}, {dbNameStr} from {clientType}";
                    break;
                case ConnectionStatus.Info:
                    return $"[INFO] Update for {subsType}, {dbNameStr}, with {clientType}: {info}";
                default:
                    throw new ArgumentOutOfRangeException(nameof(status), status, null);
            }

            if (info == null)
                return m;
            return $"{m}, {info}";
        }

        public virtual void FinishProcessing()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Finished processing subscription"));

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Finished processing subscription {SubscriptionId} / from client {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }
        }

        public void AddToStatusDescription(string message)
        {
            while (RecentSubscriptionStatuses.Count > 50)
            {
                RecentSubscriptionStatuses.TryDequeue(out _);
            }

            RecentSubscriptionStatuses.Enqueue(message);
        }

        public virtual async Task ParseSubscriptionOptionsAsync()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (BlittableJsonReaderObject subscriptionCommandOptions = await context.ParseToMemoryAsync(
                       TcpConnection.Stream,
                       "subscription options",
                       BlittableJsonDocumentBuilder.UsageMode.None,
                       _copiedBuffer.Buffer,
                       token: CancellationTokenSource.Token))
            {
                _options = JsonDeserializationServer.SubscriptionConnectionOptions(subscriptionCommandOptions);

                if (string.IsNullOrEmpty(_options.SubscriptionName))
                    return;

                context.OpenReadTransaction();

                var subscriptionItemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, _options.SubscriptionName);
                var translation = ServerStore.Cluster.Read(context, subscriptionItemKey);
                if (translation == null)
                    throw new SubscriptionDoesNotExistException("Cannot find any Subscription Task with name: " + _options.SubscriptionName);

                if (translation.TryGet(nameof(Client.Documents.Subscriptions.SubscriptionState.SubscriptionId), out long id) == false)
                    throw new SubscriptionClosedException("Could not figure out the Subscription Task ID for subscription named: " + _options.SubscriptionName);

                SubscriptionId = id;
            }
        }

        private async Task WriteJsonAsync(DynamicJsonValue value)
        {
            int writtenBytes;
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, TcpConnection.Stream))
            {
                context.Write(writer, value);
                writtenBytes = writer.Position;
            }

            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(writtenBytes);
        }

        public Task SendAcceptMessageAsync() => WriteJsonAsync(AcceptMessage());

        protected virtual DynamicJsonValue AcceptMessage()
        {
            return new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            };
        }

        internal async Task SendHeartBeatIfNeededAsync(Stopwatch sp, string reason)
        {
            if (sp.ElapsedMilliseconds >= ISubscriptionConnection.WaitForChangedDocumentsTimeoutInMs)
            {
                await SendHeartBeatAsync(reason);
                sp.Restart();
            }
        }

        public async Task SendHeartBeatAsync(string reason)
        {
            try
            {
                await TcpConnection.Stream.WriteAsync(Heartbeat, 0, Heartbeat.Length, CancellationTokenSource.Token);
                await TcpConnection.Stream.FlushAsync();

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Subscription {Options.SubscriptionName} is sending a Heartbeat message to the client. Reason: {reason}");
                }
            }
            catch (Exception ex)
            {
                throw new SubscriptionClosedException($"Cannot contact client anymore, closing subscription ({Options?.SubscriptionName})",
                    canReconnect: ex is OperationCanceledException, ex);
            }

            TcpConnection.RegisterBytesSent(Heartbeat.Length);
        }

        private async Task WaitForClientAck()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Waiting for acknowledge from client."));

            SubscriptionConnectionClientMessage clientReply;
            while (true)
            {
                var result = await Task.WhenAny(_lastReplyFromClientTask, TimeoutManager.WaitFor(ISubscriptionConnection.HeartbeatTimeout, CancellationTokenSource.Token));
                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                if (result == _lastReplyFromClientTask)
                {
                    clientReply = await _lastReplyFromClientTask;
                    if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                    {
                        CancellationTokenSource.Cancel();
                        break;
                    }

                    _lastReplyFromClientTask = GetReplyFromClientAsync();
                    break;
                }

                await SendHeartBeatAsync("Waiting for client ACK");
                await SendNoopAckAsync();
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    {
                        AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Got acknowledge from client."));
                        await OnClientAckAsync(clientReply.ChangeVector);
                        break;
                    }
                //precaution, should not reach this case...
                case SubscriptionConnectionClientMessage.MessageType.DisposedNotification:
                    CancellationTokenSource.Cancel();
                    break;

                default:
                    throw new ArgumentException("Unknown message type from client " +
                                                clientReply.Type);
            }
        }

        protected async Task SendConfirmAsync(DateTime time)
        {
            Stats.Metrics.LastAckReceivedAt = time;
            Stats.Metrics.AckRate?.Mark();
            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
            });
        }

        protected abstract StatusMessageDetails GetStatusMessageDetails();

        protected abstract Task OnClientAckAsync(string clientReplyChangeVector);
        public abstract Task SendNoopAckAsync();
        protected abstract bool FoundAboutMoreDocs();
        protected abstract SubscriptionConnectionInUse MarkInUse();

        protected abstract void AfterProcessorCreation();
        protected abstract void RaiseNotificationForBatchEnd(string name, SubscriptionBatchStatsAggregator last);
        protected abstract Task<SubscriptionBatchStatus> TryRecordBatchAndUpdateStatusAsync(IChangeVectorOperationContext context, SubscriptionBatchResult result);

        internal static void WriteEndOfBatch(AsyncBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(SubscriptionConnectionServerMessage.Type));
            writer.WriteString(nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch));
            writer.WriteEndObject();
        }

        internal static async Task FlushDocsToClientAsync(long subscriptionId, AsyncBlittableJsonTextWriter writer, MemoryStream buffer,
            TcpConnectionOptions tcpConnection, SubscriptionConnectionMetrics metrics, Logger logger, int flushedDocs, bool endOfBatch = false,
            CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (logger is { IsInfoEnabled: true })
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

        internal static async Task FlushBufferToNetworkAsync(MemoryStream buffer, TcpConnectionOptions tcpConnection, CancellationToken token = default)
        {
            buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await tcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count, token);
            await tcpConnection.Stream.FlushAsync(token);
            tcpConnection.RegisterBytesSent(bytes.Count);
            buffer.SetLength(0);
        }

        protected virtual void DisposeInternal()
        {

            using (_copiedBuffer.ReleaseBuffer)
            {
                try
                {
                    CancellationTokenSource.Cancel();
                }
                catch
                {
                    // ignored
                }

                try
                {
                    _tcpConnectionDisposable?.Dispose();
                }
                catch
                {
                    // ignored
                }

                try
                {
                    TcpConnection.Dispose();
                }
                catch
                {
                    // ignored
                }

                try
                {
                    CancellationTokenSource.Dispose();
                }
                catch
                {
                    // ignored
                }

                try
                {
                    if (_lastReplyFromClientTask is { IsCompleted: false })
                    {
                        // it's supposed this task will fail here since we disposed all resources used by connection
                        // but we must wait for it before we release _copiedBuffer
                        _lastReplyFromClientTask.Wait();
                    }
                }
                catch
                {
                    // ignored
                }

                try
                {
                    _buffer.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }

                RecentSubscriptionStatuses?.Clear();
            }

            Stats?.Dispose();
        }

        public void Dispose()
        {
            _disposeOnce.Dispose();
        }

        public sealed class SubscriptionConnectionInUse : IDisposable
        {
            private readonly IDisposable _release;
            private bool _disposed;

            public SubscriptionConnectionInUse(IDisposable release)
            {
                _release = release;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;

                try
                {
                    _release?.Dispose();
                }
                finally
                {
                    _disposed = true;
                }
            }
        }
    }
}
