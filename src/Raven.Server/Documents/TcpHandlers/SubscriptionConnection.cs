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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Esprima;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Json;
using Raven.Server.Rachis;
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

    public class SubscriptionConnection : IDisposable
    {
        public static long NonExistentBatch = -1;
        internal static int WaitForChangedDocumentsTimeoutInMs = 3000;
        private static readonly int MaxBatchSizeInBytes = Constants.Size.Megabyte;
        private static readonly int MaxBufferCapacityInBytes = 2 * MaxBatchSizeInBytes;
        private static readonly StringSegment DataSegment = new StringSegment("Data");
        private static readonly StringSegment IncludesSegment = new StringSegment(nameof(QueryResult.Includes));
        private static readonly StringSegment CounterIncludesSegment = new StringSegment(nameof(QueryResult.CounterIncludes));
        private static readonly StringSegment TimeSeriesIncludesSegment = new StringSegment(nameof(QueryResult.TimeSeriesIncludes));
        private static readonly StringSegment IncludedCounterNamesSegment = new StringSegment(nameof(QueryResult.IncludedCounterNames));
        private static readonly StringSegment ExceptionSegment = new StringSegment("Exception");
        private static readonly StringSegment TypeSegment = new StringSegment("Type");
        private static readonly TimeSpan InitialConnectionTimeout = TimeSpan.FromMilliseconds(16);

        private readonly ServerStore _server;
        public readonly TcpConnectionOptions TcpConnection;
        public readonly string ClientUri;
        private readonly MemoryStream _buffer = new MemoryStream();
        private  Logger _logger;
        public readonly SubscriptionConnectionStats Stats;

        private SubscriptionConnectionStatsScope _connectionScope;
        private SubscriptionConnectionStatsScope _pendingConnectionScope;
        private SubscriptionConnectionStatsScope _activeConnectionScope;

        private static int _connectionStatsId;
        private int _connectionStatsIdForConnection;
        private static int _batchStatsId;
        private Task<SubscriptionConnectionClientMessage> _lastReplyFromClientTask;

        private SubscriptionConnectionStatsAggregator _lastConnectionStats; // inProgress connection data
        public SubscriptionConnectionStatsAggregator GetPerformanceStats()
        {
            return _lastConnectionStats;
        }

        private SubscriptionBatchStatsAggregator _lastBatchStats; // inProgress batch data
        public SubscriptionBatchStatsAggregator GetBatchPerformanceStats()
        {
            return _lastBatchStats;
        }

        private readonly ConcurrentQueue<SubscriptionBatchStatsAggregator> _lastBatchesStats = new ConcurrentQueue<SubscriptionBatchStatsAggregator>(); // batches history
        public List<SubscriptionBatchStatsAggregator> GetBatchesPerformanceStats()
        {
            return _lastBatchesStats.ToList();
        }

        public readonly CancellationTokenSource CancellationTokenSource;

        private SubscriptionWorkerOptions _options;

        public string WorkerId => _options.WorkerId ??= Guid.NewGuid().ToString();
        public SubscriptionWorkerOptions Options => _options;

        public SubscriptionException ConnectionException;

        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");

        private SubscriptionConnectionsState _subscriptionConnectionsState;
        public SubscriptionConnectionsState SubscriptionConnectionsState => _subscriptionConnectionsState;

        public long CurrentBatchId;

        public string LastSentChangeVectorInThisConnection;

        private bool _isDisposed;
        public SubscriptionState SubscriptionState;

        public ParsedSubscription Subscription;

        public long SubscriptionId { get; set; }
        public SubscriptionOpeningStrategy Strategy => _options.Strategy;

        public readonly ConcurrentQueue<string> RecentSubscriptionStatuses = new ConcurrentQueue<string>();

        public void AddToStatusDescription(string message)
        {
            while (RecentSubscriptionStatuses.Count > 50)
            {
                RecentSubscriptionStatuses.TryDequeue(out _);
            }

            RecentSubscriptionStatuses.Enqueue(message);
        }

        public SubscriptionConnection(ServerStore serverStore, TcpConnectionOptions connectionOptions, IDisposable subscriptionConnectionInProgress,
            JsonOperationContext.MemoryBuffer bufferToCopy)
        {
            _server = serverStore;
            TcpConnection = connectionOptions;
            _subscriptionConnectionInProgress = subscriptionConnectionInProgress;
            ClientUri = connectionOptions.TcpClient.Client.RemoteEndPoint.ToString();
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnection>(connectionOptions.DocumentDatabase.Name);
            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(TcpConnection.DocumentDatabase.DatabaseShutdown);
            _supportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Subscription, connectionOptions.ProtocolVersion);
            Stats = new SubscriptionConnectionStats();

            _copiedBuffer = bufferToCopy.Clone(connectionOptions.ContextPool);

            _connectionStatsIdForConnection = Interlocked.Increment(ref _connectionStatsId);

            CurrentBatchId = NonExistentBatch;
        }

        private async Task ParseSubscriptionOptionsAsync()
        {
            using (TcpConnection.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                
                _logger = LoggingSource.Instance.GetLogger(TcpConnection.DocumentDatabase.Name, $"{nameof(SubscriptionConnection)}<{_options.SubscriptionName}>");
                context.OpenReadTransaction();

                var subscriptionItemKey = Client.Documents.Subscriptions.SubscriptionState.GenerateSubscriptionItemKeyName(TcpConnection.DocumentDatabase.Name, _options.SubscriptionName);
                var translation = TcpConnection.DocumentDatabase.ServerStore.Cluster.Read(context, subscriptionItemKey);
                if (translation == null)
                    throw new SubscriptionDoesNotExistException("Cannot find any Subscription Task with name: " + _options.SubscriptionName);

                if (translation.TryGet(nameof(Client.Documents.Subscriptions.SubscriptionState.SubscriptionId), out long id) == false)
                    throw new SubscriptionClosedException("Could not figure out the Subscription Task ID for subscription named: " + _options.SubscriptionName);

                SubscriptionId = id;
            }
        }

        private async Task InitAsync()
        {
            await ParseSubscriptionOptionsAsync();

            var message = $"A connection for subscription ID {SubscriptionId} was received from remote IP {TcpConnection.TcpClient.Client.RemoteEndPoint}";
            AddToStatusDescription(message);
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(message);
            }

            // first, validate details and make sure subscription exists
            SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, registerConnectionDurationInTicks: null, CancellationTokenSource.Token);

            Subscription = ParseSubscriptionQuery(SubscriptionState.Query);

            AssertSupportedFeatures();
        }

        private async Task NotifyClientAboutSuccess(long registerConnectionDurationInTicks)
        {
            TcpConnection.DocumentDatabase.ForTestingPurposes?.Subscription_ActionToCallAfterRegisterSubscriptionConnection?.Invoke(registerConnectionDurationInTicks);

            _activeConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionActive);

            // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)
            SubscriptionState =
                await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName, registerConnectionDurationInTicks,
                    CancellationTokenSource.Token);

            Subscription = ParseSubscriptionQuery(SubscriptionState.Query);

            // update the state if above data changed
            await _subscriptionConnectionsState.InitializeAsync(this, afterSubscribe: true);

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            await TcpConnection.DocumentDatabase.SubscriptionStorage.UpdateClientConnectionTime(SubscriptionState.SubscriptionId,
                SubscriptionState.SubscriptionName, SubscriptionState.MentorNode);

            await SubscriptionConnectionsState.SendNoopAck();
            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            });
        }

        private async Task<(IDisposable DisposeOnDisconnect, long RegisterConnectionDurationInTicks)> SubscribeAsync(Stopwatch registerConnectionDuration)
        {
            var random = new Random();
            var sp = Stopwatch.StartNew();
            while (true)
            {
                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                try
                {
                    var disposeOnce = _subscriptionConnectionsState.RegisterSubscriptionConnection(this);
                    registerConnectionDuration.Stop();
                    return (disposeOnce, registerConnectionDuration.ElapsedTicks);
                }
                catch (TimeoutException)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"A connection from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} is starting to wait until previous connection from " +
                            $"{_subscriptionConnectionsState.GetConnectionsAsString()} is released");
                    }

                    var timeout = TimeSpan.FromMilliseconds(Math.Max(250, (long)_options.TimeToWaitBeforeConnectionRetry.TotalMilliseconds / 2) + random.Next(15, 50));
                    await Task.Delay(timeout);
                    await SendHeartBeatIfNeededAsync(sp,
                        $"A connection from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} is waiting for Subscription Task that is serving a connection from IP " +
                        $"{_subscriptionConnectionsState.GetConnectionsAsString()} to be released");
                }
            }
        }

        private async Task<IDisposable> AddPendingConnectionUnderLockAsync()
        {
             var connectionInfo = new SubscriptionConnectionInfo(this);
            _connectionScope.RecordConnectionInfo(SubscriptionState, ClientUri, _options.Strategy, WorkerId);

            _subscriptionConnectionsState._pendingConnections.Add(connectionInfo);

            try
            {
                await _subscriptionConnectionsState.TakeConcurrentConnectionLockAsync(this);
            }
            catch
            {
                _subscriptionConnectionsState._pendingConnections.TryRemove(connectionInfo);
                throw;
            }

            return new DisposableAction(() =>
            {
                _subscriptionConnectionsState._pendingConnections.TryRemove(connectionInfo);
                _subscriptionConnectionsState.ReleaseConcurrentConnectionLock(this);
            });
        }

        internal async Task SendHeartBeatIfNeededAsync(Stopwatch sp, string reason)
        {
            if (sp.ElapsedMilliseconds >= WaitForChangedDocumentsTimeoutInMs)
            {
                await SendHeartBeatAsync(reason);
                sp.Restart();
            }
        }

        private void AssertSupportedFeatures()
        {
            if (_supportedFeatures.Subscription.Includes == false)
            {
                if (Subscription.Includes != null && Subscription.Includes.Length > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Includes.");
            }

            if (_supportedFeatures.Subscription.CounterIncludes == false)
            {
                if (Subscription.CounterIncludes != null && Subscription.CounterIncludes.Length > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support Counter Includes.");
            }

            if (_supportedFeatures.Subscription.TimeSeriesIncludes == false)
            {
                if (Subscription.TimeSeriesIncludes != null && Subscription.TimeSeriesIncludes.TimeSeries.Count > 0)
                    throw new SubscriptionInvalidStateException(
                        $"A connection to Subscription Task with ID '{SubscriptionId}' cannot be opened because it requires the protocol to support TimeSeries Includes.");
            }

            if (_options.Strategy == SubscriptionOpeningStrategy.Concurrent)
            {
                _server.LicenseManager.AssertCanAddConcurrentDataSubscriptions();
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

        private async Task FlushBufferToNetwork()
        {
            _buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await TcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);
            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(bytes.Count);
            _buffer.SetLength(0);
        }

        public async Task Run(TcpConnectionOptions tcpConnectionOptions, IDisposable subscriptionConnectionInProgress)
        {
            using (tcpConnectionOptions)
            using (subscriptionConnectionInProgress)
            using (this)
            {
                _lastConnectionStats = new SubscriptionConnectionStatsAggregator(_connectionStatsIdForConnection, null);
                _connectionScope = _lastConnectionStats.CreateScope();

                _pendingConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionPending);
                IDisposable disposeOnDisconnect = default;

                try
                {
                    if (TcpConnection.DocumentDatabase.SubscriptionStorage.TryEnterSemaphore() == false)
                    {
                        throw new SubscriptionClosedException(
                            $"Cannot open new subscription connection, max amount of concurrent connections reached ({TcpConnection.DocumentDatabase.Configuration.Subscriptions.MaxNumberOfConcurrentConnections}), you can modify the value at 'Subscriptions.MaxNumberOfConcurrentConnections'");
                    }

                    try
                    {
                        using (_pendingConnectionScope)
                        {
                            await InitAsync();
                            _subscriptionConnectionsState = await TcpConnection.DocumentDatabase.SubscriptionStorage.OpenSubscriptionAsync(this);
                            var registerConnectionDuration = Stopwatch.StartNew();
                            using (await AddPendingConnectionUnderLockAsync())
                            {
                                (disposeOnDisconnect, long registerConnectionDurationInTicks) = await SubscribeAsync(registerConnectionDuration);
                                _pendingConnectionScope.Dispose();
                                await NotifyClientAboutSuccess(registerConnectionDurationInTicks);
                            }
                        }

                        await ProcessSubscriptionAsync();
                    }
                    finally
                    {
                        TcpConnection.DocumentDatabase.SubscriptionStorage.ReleaseSubscriptionsSemaphore();
                    }
                }
                catch (SubscriptionChangeVectorUpdateConcurrencyException e)
                {
                    _connectionScope.RecordException(SubscriptionError.Error, e.ToString());
                    _subscriptionConnectionsState.DropSubscription(e);
                    await ReportException(e);
                }
                catch (SubscriptionInUseException e)
                {
                    _connectionScope.RecordException(SubscriptionError.ConnectionRejected, e.ToString());
                    await ReportException(e);
                }
                catch (Exception e)
                {
                    _connectionScope.RecordException(SubscriptionError.Error, e.ToString());
                    await ReportException(e);
                }
                finally
                {
                    AddToStatusDescription("Finished processing subscription");
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Finished processing subscription {SubscriptionId} / from client {TcpConnection.TcpClient.Client.RemoteEndPoint}");
                    }
                    disposeOnDisconnect?.Dispose();
                }
            }
        }

        private async Task ReportException(Exception e)
        {
            var errorMessage = $"Failed to process subscription {SubscriptionId} / from client {TcpConnection.TcpClient.Client.RemoteEndPoint}";
            AddToStatusDescription($"{errorMessage}. Sending response to client");
            if (_logger.IsInfoEnabled && e is not OperationCanceledException)
            {
                _logger.Info(errorMessage, e);
            }

            if (ConnectionException == null && e is SubscriptionException se)
            {
                ConnectionException = se;
            }

            try
            {
                await ReportExceptionToClient(_server, this, ConnectionException ?? e);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public Task SubscriptionConnectionTask;

        public static void SendSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var subscriptionConnectionInProgress = tcpConnectionOptions.ConnectionProcessingInProgress("Subscription");

            try
            {
                var connection = new SubscriptionConnection(server, tcpConnectionOptions, subscriptionConnectionInProgress, buffer);

                try
                {
                    connection.SubscriptionConnectionTask = connection.Run(tcpConnectionOptions, subscriptionConnectionInProgress);
                }
                catch (Exception)
                {
                    connection?.Dispose();
                    throw;
                }
            }
            catch (Exception)
            {
                subscriptionConnectionInProgress?.Dispose();

                throw;
            }
        }

        private static async Task ReportExceptionToClient(ServerStore server, SubscriptionConnection connection, Exception ex, int recursionDepth = 0)
        {
            if (recursionDepth == 2)
                return;
            try
            {
                switch (ex)
                {
                    case SubscriptionDoesNotExistException:
                    case DatabaseDoesNotExistException:
                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.NotFound),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionClosedException sce:
                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Closed),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString(),
                            [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                            {
                                [nameof(SubscriptionClosedException.CanReconnect)] = sce.CanReconnect
                            }
                        });
                        break;
                    case SubscriptionInvalidStateException:
                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionInUseException:
                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.InUse),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case SubscriptionDoesNotBelongToNodeException subscriptionDoesNotBelongException:
                        if (string.IsNullOrEmpty(subscriptionDoesNotBelongException.AppropriateNode) == false)
                        {
                            try
                            {
                                using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                                using (ctx.OpenReadTransaction())
                                {
                                    // check that the subscription exists on AppropriateNode
                                    var clusterTopology = server.GetClusterTopology(ctx);
                                    using (var requester = ClusterRequestExecutor.CreateForShortTermUse(
                                    clusterTopology.GetUrlFromTag(subscriptionDoesNotBelongException.AppropriateNode), server.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
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

                        connection.AddToStatusDescription("Redirecting subscription client to different server");
                        if (connection._logger.IsInfoEnabled)
                        {
                            connection._logger.Info("Subscription does not belong to current node", ex);
                        }

                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Redirect),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString(),
                            [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                            {
                                [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)] = subscriptionDoesNotBelongException.AppropriateNode,
                                [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.CurrentTag)] =
                                    connection.TcpConnection.DocumentDatabase.ServerStore.NodeTag,
                                [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RegisterConnectionDurationInTicks)] = subscriptionDoesNotBelongException.RegisterConnectionDurationInTicks,
                                [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.Reasons)] =
                                    new DynamicJsonArray(subscriptionDoesNotBelongException.Reasons.Select(item => new DynamicJsonValue
                                    {
                                        [item.Key] = item.Value
                                    }))
                            }
                        });
                        break;
                    case SubscriptionChangeVectorUpdateConcurrencyException subscriptionConcurrency:
                        connection.AddToStatusDescription("Subscription change vector update concurrency error");
                        if (connection._logger.IsInfoEnabled)
                        {
                            connection._logger.Info("Subscription change vector update concurrency error", ex);
                        }

                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.ConcurrencyReconnect),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case LicenseLimitException licenseLimitException:
                        await connection.WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                            [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                            [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                            [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                        });
                        break;
                    case RachisApplyException { InnerException: SubscriptionException } commandExecution:
                        await ReportExceptionToClient(server, connection, commandExecution.InnerException, recursionDepth - 1);
                        break;
                    default:
                        if (ex is not OperationCanceledException)
                        {
                            connection.AddToStatusDescription("Subscription error");

                            if (connection._logger.IsInfoEnabled)
                            {
                                connection._logger.Info("Subscription error", ex);
                            }
                        }
                        await connection.WriteJsonAsync(new DynamicJsonValue
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

        private async Task<SubscriptionConnectionClientMessage> GetReplyFromClientAsync()
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
                    TcpConnection._lastEtagReceived = -1;
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
                if (_isDisposed == false)
                    throw;

                return new SubscriptionConnectionClientMessage
                {
                    ChangeVector = null,
                    Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification
                };
            }
            catch (ObjectDisposedException)
            {
                return new SubscriptionConnectionClientMessage
                {
                    ChangeVector = null,
                    Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification
                };
            }
        }

        private readonly IDisposable _subscriptionConnectionInProgress;
        private readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;
        private readonly TcpConnectionHeaderMessage.SupportedFeatures _supportedFeatures;
        private SubscriptionProcessor _processor;

        private async Task ProcessSubscriptionAsync()
        {
            AddToStatusDescription("Starting to process subscription");
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting processing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            using (_processor = SubscriptionProcessor.Create(this))
            {
                var replyFromClientTask = _lastReplyFromClientTask = GetReplyFromClientAsync();

                _processor.AddScript(SetupFilterAndProjectionScript());

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

                                var anyDocumentsSentInCurrentIteration =
                                    await TrySendingBatchToClient(docsContext, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats);

                                if (anyDocumentsSentInCurrentIteration == false)
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info($"Did not find any documents to send for subscription {Options.SubscriptionName}");
                                    }

                                    AddToStatusDescription(
                                        $"Acknowledging docs processing progress without sending any documents to client. CV: {_subscriptionConnectionsState.LastChangeVectorSent ?? "None"}");

                                    if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion < 53_000)
                                    {
                                        await TcpConnection.DocumentDatabase.SubscriptionStorage.LegacyAcknowledgeBatchProcessed(
                                            SubscriptionId,
                                            Options.SubscriptionName,
                                            LastSentChangeVectorInThisConnection ?? nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                                            SubscriptionConnectionsState.LastChangeVectorSent); // the last cv we acked

                                        _subscriptionConnectionsState.LastChangeVectorSent =
                                            ChangeVectorUtils.MergeVectors(_subscriptionConnectionsState.LastChangeVectorSent, LastSentChangeVectorInThisConnection);
                                    }

                                    UpdateBatchPerformanceStats(0, false);

                                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                        await SendHeartBeatAsync("Didn't find any documents to send and more then 1000ms passed");

                                    using (docsContext.OpenReadTransaction())
                                    {
                                        var globalEtag = _processor.GetLastItemEtag(docsContext, Subscription.Collection);
                                        if (globalEtag > SubscriptionConnectionsState.GetLastEtagSent())
                                        {
                                            _subscriptionConnectionsState.NotifyHasMoreDocs();
                                        }
                                    }

                                    AssertCloseWhenNoDocsLeft();

                                    // we might wait for new documents for a long times, lets reduce the stream capacity
                                    if (_buffer.Capacity > MaxBufferCapacityInBytes)
                                    {
                                        Debug.Assert(_buffer.Length <= MaxBufferCapacityInBytes, $"{_buffer.Length} <= {MaxBufferCapacityInBytes}");
                                        _buffer.Capacity = MaxBufferCapacityInBytes;
                                    }

                                    if (await WaitForChangedDocs(replyFromClientTask))
                                        continue;
                                }
                            }

                            using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                            {
                                replyFromClientTask = await WaitForClientAck(replyFromClientTask);
                            }

                            UpdateBatchPerformanceStats(batchScope.GetBatchSize());
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

        private void UpdateBatchPerformanceStats(long batchSize, bool anyDocumentsSent = true)
        {
            _lastBatchStats.Complete();

            if (anyDocumentsSent)
            {
                _connectionScope.RecordBatchCompleted(batchSize);

                AddBatchPerformanceStatsToBatchesHistory(_lastBatchStats);
                TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForBatchEnded(_options.SubscriptionName, _lastBatchStats);
            }

            _lastBatchStats = null;
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

        private async Task<Task<SubscriptionConnectionClientMessage>> WaitForClientAck(Task<SubscriptionConnectionClientMessage> replyFromClientTask)
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

                    replyFromClientTask = _lastReplyFromClientTask = GetReplyFromClientAsync();
                    break;
                }

                await SendHeartBeatAsync("Waiting for client ACK");
                await SubscriptionConnectionsState.SendNoopAck();
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    {
                        if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= 53_000)
                        {
                            await _processor.AcknowledgeBatch(CurrentBatchId);
                        }
                        else
                        {
                            await TcpConnection.DocumentDatabase.SubscriptionStorage.LegacyAcknowledgeBatchProcessed(
                                SubscriptionId,
                                Options.SubscriptionName,
                                LastSentChangeVectorInThisConnection,
                                SubscriptionConnectionsState.LastChangeVectorSent);

                            //since we send the next batch by LastChangeVectorSent, in legacy will represent the last acked cv instead
                            _subscriptionConnectionsState.LastChangeVectorSent =
                                ChangeVectorUtils.MergeVectors(_subscriptionConnectionsState.LastChangeVectorSent, LastSentChangeVectorInThisConnection);
                        }

                        Stats.LastAckReceivedAt = TcpConnection.DocumentDatabase.Time.GetUtcNow();
                        Stats.AckRate?.Mark();
                        await WriteJsonAsync(new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
                        });

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

            return replyFromClientTask;
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
                AddToStatusDescription("Start trying to send docs to client");
                bool anyDocumentsSentInCurrentIteration = false;

                using (batchScope.For(SubscriptionOperationScope.BatchSendDocuments))
                {
                    batchScope.RecordBatchInfo(_subscriptionConnectionsState.SubscriptionId, _subscriptionConnectionsState.SubscriptionName, _connectionStatsIdForConnection,
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
                                    ChangeVectorUtils.NewChangeVector(SubscriptionConnectionsState.DocumentDatabase, result.Doc.Etag),
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
                                        docsContext.ReadObject(new DynamicJsonValue { [Client.Constants.Documents.Metadata.Key] = metadata }, result.Doc.Id)
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
                                if (_buffer.Length > MaxBatchSizeInBytes ||
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
                                    var size = writer.WriteTimeSeries(includeTimeSeriesCommand.Results);

                                    batchScope.RecordIncludedTimeSeriesInfo(includeTimeSeriesCommand.Results.Sum(x =>
                                    x.Value.Sum(y => y.Value.Sum(z => z.Entries.Length))), size);

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

                            if (lastChangeVectorSentInThisBatch != null)
                            {
                                //Entire unsent batch could contain docs that have to be skipped, but we still want to update the etag in the cv
                                LastSentChangeVectorInThisConnection = lastChangeVectorSentInThisBatch;

                                if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= 53_000)
                                {
                                    CurrentBatchId = await _processor.RecordBatch(lastChangeVectorSentInThisBatch);

                                    _subscriptionConnectionsState.LastChangeVectorSent =
                                        ChangeVectorUtils.MergeVectors(_subscriptionConnectionsState.LastChangeVectorSent, lastChangeVectorSentInThisBatch);
                                    _subscriptionConnectionsState.PreviouslyRecordedChangeVector =
                                        ChangeVectorUtils.MergeVectors(_subscriptionConnectionsState.PreviouslyRecordedChangeVector, lastChangeVectorSentInThisBatch);
                                }
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

        internal async Task SendHeartBeatAsync(string reason)
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
                throw new SubscriptionClosedException($"Cannot contact client anymore, closing subscription ({Options?.SubscriptionName})", canReconnect: ex is OperationCanceledException, ex);
            }

            TcpConnection.RegisterBytesSent(Heartbeat.Length);
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

        private async Task<bool> WaitForChangedDocs(Task pendingReply)
        {
            AddToStatusDescription("Start waiting for changed documents");
            do
            {
                var hasMoreDocsTask = _subscriptionConnectionsState.WaitForMoreDocs();
                var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(WaitForChangedDocumentsTimeoutInMs));
                var resultingTask = await Task
                    .WhenAny(hasMoreDocsTask, pendingReply, timeoutTask).ConfigureAwait(false);

                TcpConnection.DocumentDatabase.ForTestingPurposes?.Subscription_ActionToCallDuringWaitForChangedDocuments?.Invoke();

                if (CancellationTokenSource.IsCancellationRequested)
                    return false;

                if (resultingTask == pendingReply)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                {
                    if (TcpConnection.DocumentDatabase.SubscriptionStorage.ShouldWaitForClusterStabilization())
                    {
                        // we have unstable cluster
                        await timeoutTask;
                    }
                    else
                    {
                        _subscriptionConnectionsState.NotifyNoMoreDocs();
                        return true;
                    }
                }

                await SendHeartBeatAsync("Waiting for changed documents");
                await SubscriptionConnectionsState.SendNoopAck();
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

        public void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            _lastConnectionStats.Complete();
            TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForConnectionEnded(this);

            using (_copiedBuffer.ReleaseBuffer)
            {
                try
                {
                    _subscriptionConnectionInProgress?.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }

                try
                {
                    TcpConnection.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }

                try
                {
                    CancellationTokenSource.Cancel();
                }
                catch (Exception)
                {
                    // ignored
                }

                try
                {
                    CancellationTokenSource.Dispose();
                }
                catch (Exception)
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

                Stats.Dispose();

                RecentSubscriptionStatuses?.Clear();

                _pendingConnectionScope?.Dispose();
                _activeConnectionScope?.Dispose();
                _connectionScope.Dispose();

                try
                {
                   _buffer.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }
            }
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

        private void AddBatchPerformanceStatsToBatchesHistory(SubscriptionBatchStatsAggregator batchStats)
        {
            _lastBatchesStats.Enqueue(batchStats); // add to batches history

            while (_lastBatchesStats.Count > 25)
                _lastBatchesStats.TryDequeue(out batchStats);
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
