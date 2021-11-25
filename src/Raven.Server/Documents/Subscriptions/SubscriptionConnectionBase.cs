// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionBase.cs" company="Hibernating Rhinos LTD">
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
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionConnectionBase : IDisposable
    {
        private static readonly TimeSpan InitialConnectionTimeout = TimeSpan.FromMilliseconds(16);
        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");
        internal static int _connectionStatsId;

        private readonly IDisposable _tcpConnectionDisposable;
        private readonly ServerStore _serverStore;
        private readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;
        private SubscriptionConnectionStatsAggregator _lastConnectionStats; // inProgress connection data

        internal SubscriptionConnectionStatsScope _connectionScope;
        internal SubscriptionConnectionStatsScope _pendingConnectionScope;
        internal SubscriptionConnectionStatsScope _activeConnectionScope;
        internal SubscriptionWorkerOptions _options;
        internal SubscriptionConnectionState _connectionState;
        internal int _connectionStatsIdForConnection;
        internal bool _isDisposed;
        internal readonly Logger _logger;

        public readonly CancellationTokenSource CancellationTokenSource;
        public readonly string ClientUri;
        public readonly SubscriptionConnectionStats Stats;
        public readonly ConcurrentQueue<string> RecentSubscriptionStatuses = new ConcurrentQueue<string>();
        public readonly TcpConnectionOptions TcpConnection;

        public SubscriptionWorkerOptions Options => _options;
        public SubscriptionException ConnectionException;
        public SubscriptionState SubscriptionState;
        public DisposeOnce<SingleAttempt> DisposeOnDisconnect;
        public long SubscriptionId { get; set; }
        public SubscriptionOpeningStrategy Strategy => Options.Strategy;
        public string Database;
        public ShardData Shard;

        protected SubscriptionConnectionBase(TcpConnectionOptions tcpConnection, ServerStore serverStore, JsonOperationContext.MemoryBuffer memoryBuffer, IDisposable tcpConnectionDisposable,
            string database, CancellationTokenSource cts)
        {
            TcpConnection = tcpConnection;
            ClientUri = tcpConnection.TcpClient.Client.RemoteEndPoint.ToString();
            Stats = new SubscriptionConnectionStats();
            _connectionStatsIdForConnection = Interlocked.Increment(ref _connectionStatsId);
            _serverStore = serverStore;
            _copiedBuffer = memoryBuffer.Clone(serverStore.ContextPool);
            _tcpConnectionDisposable = tcpConnectionDisposable;


            Database = database;
            CancellationTokenSource = cts;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnectionBase>(database);
        }

        protected void CreateStatsScope()
        {
            _lastConnectionStats = new SubscriptionConnectionStatsAggregator(_connectionStatsId, null);
            _connectionScope = _lastConnectionStats.CreateScope();
            _pendingConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionPending);
        }

        protected async Task RecordExceptionAndReportToClient(Exception e, bool sharded)
        {
            _connectionScope.RecordException(e is SubscriptionInUseException ? SubscriptionError.ConnectionRejected : SubscriptionError.Error, e.Message);
            var subsType = sharded ? "sharded " : string.Empty;
            var clientType = sharded ? "'client worker'" : Shard == null ? "'client worker'" : "'sharded subscription worker'";
            var errorMessage = $"Failed to process {subsType}subscription '{SubscriptionId}' for database '{Database}' on node '{_serverStore.NodeTag}' / from {clientType} '{TcpConnection.TcpClient.Client.RemoteEndPoint}'";
            AddToStatusDescription($"{errorMessage}. Sending response to client");
            if (_logger.IsOperationsEnabled)
                _logger.Info(errorMessage, e);

            try
            {
                await ReportExceptionToClient(e);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        internal async Task ParseSubscriptionOptionsAsync()
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

        internal static async Task WriteJsonAsync(DynamicJsonValue value, TcpConnectionOptions tcpConnection)
        {
            int writtenBytes;
            using (tcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, tcpConnection.Stream))
            {
                context.Write(writer, value);
                writtenBytes = writer.Position;
            }

            await tcpConnection.Stream.FlushAsync();
            tcpConnection.RegisterBytesSent(writtenBytes);
        }

        internal abstract Task<string> OnClientAck(string subscriptionChangeVectorBeforeCurrentBatch);

        internal async Task<(Task<SubscriptionConnectionClientMessage> ReplyFromClientTask, string SubscriptionChangeVectorBeforeCurrentBatch)> WaitForClientAck(
            Task<SubscriptionConnectionClientMessage> replyFromClientTask, string subscriptionChangeVectorBeforeCurrentBatch, bool sharded)
        {
            SubscriptionConnectionClientMessage clientReply;
            while (true)
            {
                var result = await Task.WhenAny(replyFromClientTask, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(5000), CancellationTokenSource.Token)).ConfigureAwait(false);
                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                if (result == replyFromClientTask)
                {
                    clientReply = await replyFromClientTask;
                    if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                    {
                        CancellationTokenSource.Cancel();
                        break;
                    }

                    replyFromClientTask = sharded ? null : GetReplyFromClientAsync();
                    break;
                }

                await SendHeartBeat("Waiting for client ACK");
                if (sharded == false)
                {
                    await SendNoopAck();
                }
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();
            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    subscriptionChangeVectorBeforeCurrentBatch = await OnClientAck(subscriptionChangeVectorBeforeCurrentBatch);

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

        protected async Task SendNoopAck()
        {
            await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                Database,
                Shard,
                SubscriptionId,
                Options.SubscriptionName,
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange));
        }

        protected async Task ReportExceptionToClient(Exception ex, int recursionDepth = 0)
        {
            if (recursionDepth == 2)
                return;
            try
            {
                if (ex is SubscriptionDoesNotExistException || ex is DatabaseDoesNotExistException)
                {
                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.NotFound),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    }, TcpConnection);
                }
                else if (ex is SubscriptionClosedException sce)
                {
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
                    }, TcpConnection);
                }
                else if (ex is SubscriptionInvalidStateException)
                {
                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    }, TcpConnection);
                }
                else if (ex is SubscriptionInUseException)
                {
                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.InUse),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    }, TcpConnection);
                }
                else if (ex is SubscriptionDoesNotBelongToNodeException subscriptionDoesNotBelongException)
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

                    AddToStatusDescription("Redirecting subscription client to different server");
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
                            [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.Reasons)] =
                                new DynamicJsonArray(subscriptionDoesNotBelongException.Reasons.Select(item => new DynamicJsonValue
                                {
                                    [item.Key] = item.Value
                                }))
                        }
                    }, TcpConnection);
                }
                else if (ex is SubscriptionChangeVectorUpdateConcurrencyException)
                {
                    AddToStatusDescription($"Subscription change vector update concurrency error, reporting to '{TcpConnection.TcpClient.Client.RemoteEndPoint}'");
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
                    }, TcpConnection);
                }
                else if (ex is RachisApplyException commandExecution && commandExecution.InnerException is SubscriptionException)
                {
                    await ReportExceptionToClient(commandExecution.InnerException, recursionDepth - 1);
                }
                else
                {
                    //TODO: egor finish
                    var dbNameStr = Shard == null ? $"for database '{Database}'" : $"for shard '{Shard.ShardName}' of database '{Database}'";
                    AddToStatusDescription($"Subscription error on subscription '{_options.SubscriptionName}' with id '{SubscriptionId}', {dbNameStr}" + this.);
                    var message = $"A connection for subscription '{_options.SubscriptionName}' with id '{SubscriptionId}', {dbNameStr} was received from {clientType} with remote IP {TcpConnection.TcpClient.Client.RemoteEndPoint}";

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
                    }, TcpConnection);
                }
            }
            catch
            {
                // ignored
            }
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

        public async Task TryConnectSubscription()
        {
            _connectionState = OpenSubscription();

            var timeout = InitialConnectionTimeout;

            bool shouldRetry;

            var random = new Random();

            _connectionScope.RecordConnectionInfo(SubscriptionState, ClientUri, _options.Strategy);

            _connectionState.PendingConnections.Add(this);

            try
            {
                do
                {
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();
                    try
                    {

                        DisposeOnDisconnect = await _connectionState.RegisterSubscriptionConnection(this, timeout);
                        shouldRetry = false;
                    }
                    catch (TimeoutException)
                    {
                        if (timeout == InitialConnectionTimeout && _logger.IsInfoEnabled)
                        {
                            _logger.Info(
                                $"A connection from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} is starting to wait until previous connection from " +
                                $"{_connectionState.Connection?.TcpConnection.TcpClient.Client.RemoteEndPoint} is released");
                        }

                        timeout = TimeSpan.FromMilliseconds(Math.Max(250, (long)_options.TimeToWaitBeforeConnectionRetry.TotalMilliseconds / 2) + random.Next(15, 50));
                        await SendHeartBeat(
                            $"A connection from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} is waiting for Subscription Task that is serving a connection from IP " +
                            $"{_connectionState.Connection?.TcpConnection.TcpClient.Client.RemoteEndPoint} to be released");
                        shouldRetry = true;
                    }

                } while (shouldRetry);
            }
            finally
            {
                _connectionState.PendingConnections.TryRemove(this);
            }

            _pendingConnectionScope.Dispose();
        }

        internal async Task SendHeartBeat(string reason)
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

        public void Dispose()
        {
            _lastConnectionStats.Complete();

            using (_copiedBuffer.ReleaseBuffer)
            {
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
                    CancellationTokenSource.Cancel();
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

                Stats.Dispose();

                RecentSubscriptionStatuses?.Clear();

                _activeConnectionScope?.Dispose();
                _connectionScope.Dispose();
            }
        }

        public abstract SubscriptionConnectionState OpenSubscription();

        public void AddToStatusDescription(string message)
        {
            Console.WriteLine(this.Database + ": "+message);
            while (RecentSubscriptionStatuses.Count > 50)
            {
                RecentSubscriptionStatuses.TryDequeue(out _);
            }
            RecentSubscriptionStatuses.Enqueue(message);
        }

        public SubscriptionConnectionStatsAggregator GetPerformanceStats()
        {
            return _lastConnectionStats;
        }
        private readonly ConcurrentQueue<SubscriptionBatchStatsAggregator> _lastBatchesStats = new ConcurrentQueue<SubscriptionBatchStatsAggregator>(); // batches history

        public List<SubscriptionBatchStatsAggregator> GetBatchesPerformanceStats()
        {
            return _lastBatchesStats.ToList();
        }

        public void UpdateBatchPerformanceStats(long batchSize, bool anyDocumentsSent = true)
        {
            if (_lastBatchStats != null)
            {
                _lastBatchStats.Complete();

                if (anyDocumentsSent)
                {
                    _connectionScope.RecordBatchCompleted(batchSize);

                    AddBatchPerformanceStatsToBatchesHistory(_lastBatchStats);
                    TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForBatchEnded(_options.SubscriptionName, _lastBatchStats);
                }
            }

            _lastBatchStats = null;
        }

        internal SubscriptionBatchStatsAggregator _lastBatchStats; // inProgress batch data
        public SubscriptionBatchStatsAggregator GetBatchPerformanceStats()
        {
            return _lastBatchStats;
        }

        private void AddBatchPerformanceStatsToBatchesHistory(SubscriptionBatchStatsAggregator batchStats)
        {
            _lastBatchesStats.Enqueue(batchStats); // add to batches history

            while (_lastBatchesStats.Count > 25)
                _lastBatchesStats.TryDequeue(out _);
        }

    }

    public class ShardData
    {
        public string DatabaseId;
        public string ShardName;
        public string LocalChangeVector;
    }

    public class SubscriptionOperationScope
    {
        public const string ConnectionPending = "ConnectionPending";
        public const string ConnectionActive = "ConnectionActive";
        public const string BatchSendDocuments = "BatchSendDocuments";
        public const string BatchWaitForAcknowledge = "BatchWaitForAcknowledge";
    }

    public enum SubscriptionError
    {
        ConnectionRejected,
        Error
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
}
