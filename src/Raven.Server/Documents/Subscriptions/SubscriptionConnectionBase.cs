using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionConnectionBase : IDisposable
    {
        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");
        public const long NonExistentBatch = -1;
        public const int WaitForChangedDocumentsTimeoutInMs = 3000;

        protected readonly ServerStore _serverStore;
        private readonly IDisposable _tcpConnectionDisposable;
        internal readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;

        internal SubscriptionWorkerOptions _options;
        internal readonly Logger _logger;

        public readonly ConcurrentQueue<string> RecentSubscriptionStatuses = new ConcurrentQueue<string>();
        internal bool _isDisposed;
        public SubscriptionWorkerOptions Options => _options;
        public SubscriptionException ConnectionException;
        public long SubscriptionId { get; set; }
        public string Database;
        public readonly TcpConnectionOptions TcpConnection;
        public readonly CancellationTokenSource CancellationTokenSource;

        public SubscriptionOpeningStrategy Strategy => _options.Strategy;
        public readonly string ClientUri;

        protected abstract Task ReportExceptionAsync(SubscriptionError error, Exception e);
        protected abstract Task OnClientAckAsync();
        protected abstract Task SendNoopAckAsync();
        internal abstract Task<SubscriptionConnectionClientMessage> GetReplyFromClientAsync();

        protected SubscriptionConnectionBase(TcpConnectionOptions tcpConnection, ServerStore serverStore, JsonOperationContext.MemoryBuffer memoryBuffer, IDisposable tcpConnectionDisposable,
            string database, CancellationToken token)
        {
            TcpConnection = tcpConnection;
            _serverStore = serverStore;
            _copiedBuffer = memoryBuffer.Clone(serverStore.ContextPool);
            _tcpConnectionDisposable = tcpConnectionDisposable;

            Database = database;
            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnectionBase>(database);

            ClientUri = tcpConnection.TcpClient.Client.RemoteEndPoint.ToString();
        }

        protected async Task LogExceptionAndReportToClientAsync(Exception e)
        {
            var errorMessage = CreateStatusMessage(ConnectionStatus.Fail, e.ToString());
            AddToStatusDescription($"{errorMessage}. Sending response to client");
            if (_logger.IsOperationsEnabled)
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
                                [nameof(SubscriptionClosedException.CanReconnect)] = sce.CanReconnect
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
                                    using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                                    using (ctx.OpenReadTransaction())
                                    {
                                        // check that the subscription exists on AppropriateNode
                                        var clusterTopology = _serverStore.GetClusterTopology(ctx);
                                        using (var requester = ClusterRequestExecutor.CreateForSingleNode(
                                                   clusterTopology.GetUrlFromTag(subscriptionDoesNotBelongException.AppropriateNode), _serverStore.Server.Certificate.Certificate))
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
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.CurrentTag)] = _serverStore.NodeTag,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RegisterConnectionDurationInTicks)] = subscriptionDoesNotBelongException.RegisterConnectionDurationInTicks,
                                    [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.Reasons)] =
                                        new DynamicJsonArray(subscriptionDoesNotBelongException.Reasons.Select(item => new DynamicJsonValue
                                        {
                                            [item.Key] = item.Value
                                        }))
                                }
                            });
                            break;
                        }
                    case SubscriptionChangeVectorUpdateConcurrencyException:
                        {
                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Subscription change vector update concurrency error, reporting to '{TcpConnection.TcpClient.Client.RemoteEndPoint}'"));
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
                    case RachisApplyException commandExecution when commandExecution.InnerException is SubscriptionException:
                        await ReportExceptionToClientAsync(commandExecution.InnerException, recursionDepth - 1);
                        break;
                    default:
                        AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Fail, $"Subscription error on subscription {ex}"));

                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info("Subscription error", ex);
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

        public class StatusMessageDetails
        {
            public string DatabaseName;
            public string ClientType;
            public string SubscriptionType;
        }

        protected abstract StatusMessageDetails GetStatusMessageDetails();

        protected string CreateStatusMessage(ConnectionStatus status, string info = null)
        {
            var message = GetStatusMessageDetails();
            var dbNameStr = message.DatabaseName;
            var clientType = message.ClientType;
            var subsType = message.SubscriptionType;

            string m = null;
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
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

                var subscriptionItemKey = SubscriptionState.GenerateSubscriptionItemKeyName(Database, _options.SubscriptionName);
                var translation = _serverStore.Cluster.Read(context, subscriptionItemKey);
                if (translation == null)
                    throw new SubscriptionDoesNotExistException("Cannot find any Subscription Task with name: " + _options.SubscriptionName);

                if (translation.TryGet(nameof(Client.Documents.Subscriptions.SubscriptionState.SubscriptionId), out long id) == false)
                    throw new SubscriptionClosedException("Could not figure out the Subscription Task ID for subscription named: " + _options.SubscriptionName);

                SubscriptionId = id;
            }
        }

        internal async Task WriteJsonAsync(DynamicJsonValue value)
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
                throw new SubscriptionClosedException($"Cannot contact client anymore, closing subscription ({Options?.SubscriptionName})", ex);
            }

            TcpConnection.RegisterBytesSent(Heartbeat.Length);
        }

        internal async Task<Task<SubscriptionConnectionClientMessage>> WaitForClientAck(Task<SubscriptionConnectionClientMessage> replyFromClientTask)
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

                    replyFromClientTask = GetReplyFromClientAsync();
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
                        await OnClientAckAsync();
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

        internal async Task<SubscriptionConnectionClientMessage> GetReplyFromClientInternalAsync()
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

        public virtual void Dispose()
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

                RecentSubscriptionStatuses?.Clear();
            }
        }
    }
}
