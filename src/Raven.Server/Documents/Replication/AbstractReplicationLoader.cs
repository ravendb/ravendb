using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Config;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Replication
{
    public abstract class AbstractReplicationLoader
    {
        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();
        private long _reconnectInProgress;
        private int _replicationStatsId;

        internal readonly ServerStore _server;
        private readonly string _databaseName;
        protected readonly Logger _logger;
        protected readonly ConcurrentDictionary<ReplicationNode, ConnectionShutdownInfo> _outgoingFailureInfo = new ConcurrentDictionary<ReplicationNode, ConnectionShutdownInfo>();
        protected readonly ConcurrentSet<ConnectionShutdownInfo> _reconnectQueue = new ConcurrentSet<ConnectionShutdownInfo>();
        protected readonly Timer _reconnectAttemptTimer;

        internal readonly int MinimalHeartbeatInterval;
        public IReadOnlyDictionary<ReplicationNode, ConnectionShutdownInfo> OutgoingFailureInfo => _outgoingFailureInfo;

        protected AbstractReplicationLoader(ServerStore serverStore, string databaseName, RavenConfiguration configuration)
        {
            _databaseName = databaseName;
            _server = serverStore;
            _logger = LoggingSource.Instance.GetLogger(GetType().FullName, databaseName);

            var config = configuration.Replication;
            var reconnectTime = config.RetryReplicateAfter.AsTimeSpan;
            _reconnectAttemptTimer = new Timer(state => ForceTryReconnectAll(),
                null, reconnectTime, reconnectTime);
            MinimalHeartbeatInterval = (int)config.ReplicationMinimalHeartbeat.AsTimeSpan.TotalMilliseconds;
        }

        protected void ForceTryReconnectAll()
        {
            if (_reconnectQueue.Count == 0)
                return;

            if (Interlocked.CompareExchange(ref _reconnectInProgress, 1, 0) == 1)
                return;

            try
            {
                DatabaseTopology topology;
                Dictionary<string, RavenConnectionString> ravenConnectionStrings;

                using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var raw = _server.Cluster.ReadRawDatabaseRecord(ctx, _databaseName);
                    if (raw == null)
                    {
                        _reconnectQueue.Clear();
                        return;
                    }

                    topology = raw.Topology;
                    ravenConnectionStrings = raw.RavenConnectionStrings;
                }

                var cts = GetCancellationToken();
                foreach (var failure in _reconnectQueue)
                {
                    if (cts.IsCancellationRequested)
                        return;

                    try
                    {
                        if (_reconnectQueue.TryRemove(failure) == false)
                            continue;

                        if (_outgoingFailureInfo.Values.Contains(failure) == false)
                            continue; // this connection is no longer exists

                        if (failure.RetryOn > DateTime.UtcNow)
                        {
                            _reconnectQueue.Add(failure);
                            continue;
                        }

                        if (failure.Node is ExternalReplicationBase exNode &&
                            IsMyTask(ravenConnectionStrings, topology, exNode) == false)
                            // no longer my task
                            continue;

                        if (failure.Node is BucketMigrationReplication migration &&
                            topology.WhoseTaskIsIt(RachisState.Follower, migration.ShardBucketMigration, getLastResponsibleNode: null) != _server.NodeTag)
                            // no longer my task
                            continue;

                        AddAndStartOutgoingReplication(failure.Node);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"Failed to start outgoing replication to {failure.Node}", e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations("Unexpected exception during ForceTryReconnectAll", e);
                }
            }
            finally
            {
                Interlocked.Exchange(ref _reconnectInProgress, 0);
            }
        }

        internal virtual void AddAndStartOutgoingReplication(ReplicationNode node)
        {
            var info = GetConnectionInfo(node);

            if (info == null)
            {
                // this means that we were unable to retrieve the tcp connection info and will try it again later
                return;
            }

            if (_locker.TryEnterReadLock(0) == false)
            {
                // the db being disposed
                return;
            }

            try
            {
                StartOutgoingReplication(info, node);
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        public int GetNextReplicationStatsId() => Interlocked.Increment(ref _replicationStatsId);

        protected bool IsMyTask(Dictionary<string, RavenConnectionString> connectionStrings, DatabaseTopology topology, ExternalReplicationBase task)
        {
            if (ValidateConnectionString(connectionStrings, task, out _) == false)
                return false;

            var taskStatus = GetExternalReplicationState(_server, _databaseName, task.TaskId);
            var whoseTaskIsIt = _server.WhoseTaskIsIt(topology, task, taskStatus);
            return whoseTaskIsIt == _server.NodeTag;
        }

        protected bool ValidateConnectionString(Dictionary<string, RavenConnectionString> ravenConnectionStrings, ExternalReplicationBase externalReplication, out RavenConnectionString connectionString)
        {
            connectionString = null;
            if (string.IsNullOrEmpty(externalReplication.ConnectionStringName))
            {
                var msg = $"The external replication {externalReplication.Name} to the database '{externalReplication.Database}' " +
                          "has an empty connection string name.";

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(msg);
                }

                _server.NotificationCenter.Add(AlertRaised.Create(
                    _databaseName,
                    "Connection string name is empty",
                    msg,
                    AlertType.Replication,
                    NotificationSeverity.Error));
                return false;
            }

            if (ravenConnectionStrings.TryGetValue(externalReplication.ConnectionStringName, out connectionString) == false)
            {
                var msg = $"Could not find connection string with name {externalReplication.ConnectionStringName} " +
                          $"for the external replication task '{externalReplication.Name}' to '{externalReplication.Database}'.";

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(msg);
                }

                _server.NotificationCenter.Add(AlertRaised.Create(
                    _databaseName,
                    "Connection string not found",
                    msg,
                    AlertType.Replication,
                    NotificationSeverity.Error));

                return false;
            }
            return true;
        }

        public static ExternalReplicationState GetExternalReplicationState(ServerStore server, string database, long taskId)
        {
            using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetExternalReplicationState(server, database, taskId, context);
            }
        }

        protected static ExternalReplicationState GetExternalReplicationState(ServerStore server, string database, long taskId, TransactionOperationContext context)
        {
            var stateBlittable = server.Cluster.Read(context, ExternalReplicationState.GenerateItemName(database, taskId));

            return stateBlittable != null ? JsonDeserializationCluster.ExternalReplicationState(stateBlittable) : new ExternalReplicationState();
        }

        protected TcpConnectionHeaderMessage.SupportedFeatures GetSupportedVersions(TcpConnectionOptions tcpConnectionOptions)
        {
            return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Replication, tcpConnectionOptions.ProtocolVersion);
        }

        protected ReplicationInitialRequest GetReplicationInitialRequest(TcpConnectionOptions tcpConnectionOptions,
            TcpConnectionHeaderMessage.SupportedFeatures supportedVersions, JsonOperationContext.MemoryBuffer buffer)
        {
            ReplicationInitialRequest initialRequest = null;
            if (supportedVersions.Replication.PullReplication)
            {
                using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var readerObject = context.Sync.ParseToMemory(tcpConnectionOptions.Stream, "initial-replication-message",
                           BlittableJsonDocumentBuilder.UsageMode.None, buffer))
                {
                    initialRequest = JsonDeserializationServer.ReplicationInitialRequest(readerObject);
                }
            }

            return initialRequest;
        }

        protected ReplicationLatestEtagRequest IncomingInitialHandshake(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer, ReplicationLoader.PullReplicationParams replParams = null)
        {
            ReplicationLatestEtagRequest getLatestEtagMessage;

            using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var readerObject = context.Sync.ParseToMemory(
                tcpConnectionOptions.Stream,
                "IncomingReplication/get-last-etag-message read",
                BlittableJsonDocumentBuilder.UsageMode.None,
                buffer))
            {
                var exceptionSchema = JsonDeserializationClient.ExceptionSchema(readerObject);
                if (exceptionSchema.Type.Equals("Error"))
                    throw new Exception(exceptionSchema.Message);

                getLatestEtagMessage = JsonDeserializationServer.ReplicationLatestEtagRequest(readerObject);
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"GetLastEtag: {getLatestEtagMessage.SourceTag}({getLatestEtagMessage.SourceMachineName}) / {getLatestEtagMessage.SourceDatabaseName} ({getLatestEtagMessage.SourceDatabaseId}) - {getLatestEtagMessage.SourceUrl}. Type: {getLatestEtagMessage.ReplicationsType}");
                }
            }

            var connectionInfo = IncomingConnectionInfo.FromGetLatestEtag(getLatestEtagMessage);
            try
            {
                AssertValidConnection(connectionInfo);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Connection from [{connectionInfo}] is rejected.", e);

                throw;
            }

            try
            {
                using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, tcpConnectionOptions.Stream))
                {
                    DynamicJsonValue response = GetInitialRequestMessage(getLatestEtagMessage, replParams);
                    context.Write(writer, response);
                    writer.Flush();
                }
            }
            catch (Exception)
            {
                try
                {
                    tcpConnectionOptions.Dispose();
                }
                catch (Exception)
                {
                    // do nothing
                }

                throw;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}");

            return getLatestEtagMessage;
        }

        public X509Certificate2 GetCertificateForReplication(ReplicationNode node, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            switch (node)
            {
                case BucketMigrationReplication _:
                case InternalReplication _:
                case ExternalReplication _:
                    authorizationInfo = null;
                    return _server.Server.Certificate.Certificate;

                case PullReplicationAsSink sink:
                    authorizationInfo = new TcpConnectionHeaderMessage.AuthorizationInfo
                    {
                        AuthorizeAs = sink.Mode switch
                        {
                            PullReplicationMode.HubToSink => TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication,
                            PullReplicationMode.SinkToHub => TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication,
                            PullReplicationMode.None => throw new ArgumentOutOfRangeException(nameof(node), "Replication mode should be set to pull or push"),
                            _ => throw new ArgumentOutOfRangeException("Unexpected replication mode: " + sink.Mode)
                        },
                        AuthorizationFor = sink.HubName
                    };

                    if (sink.CertificateWithPrivateKey == null)
                        return _server.Server.Certificate.Certificate;

                    var certBytes = Convert.FromBase64String(sink.CertificateWithPrivateKey);
                    return new X509Certificate2(certBytes, sink.CertificatePassword, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

                default:
                    throw new ArgumentException($"Unknown node type {node.GetType().FullName}");
            }
        }

        public void EnsureNotDeleted(string node)
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, _databaseName))
                {
                    if (rawRecord != null && rawRecord.DeletionInProgress.ContainsKey(node))
                    {
                        throw new OperationCanceledException($"The database '{_databaseName}' on node '{node}' is being deleted, so it will not handle replications.");
                    }
                }
            }
        }

        protected virtual void AssertValidConnection(IncomingConnectionInfo connectionInfo)
        {
            if (_server.IsPassive())
            {
                throw new InvalidOperationException(
                    $"Cannot accept the incoming replication connection from {connectionInfo.SourceUrl}, because this node is in passive state.");
            }
        }

        protected virtual DynamicJsonValue GetInitialRequestMessage(ReplicationLatestEtagRequest replicationLatestEtagRequest,
            ReplicationLoader.PullReplicationParams replParams = null)
        {
            return new DynamicJsonValue
            {
                [nameof(ReplicationMessageReply.Type)] = nameof(ReplicationMessageReply.ReplyType.Ok),
                [nameof(ReplicationMessageReply.MessageType)] = ReplicationMessageType.Heartbeat,
                [nameof(ReplicationMessageReply.NodeTag)] = _server.NodeTag,
                [nameof(ReplicationMessageReply.AcceptablePaths)] = replParams?.AllowedPaths,
                [nameof(ReplicationMessageReply.PreventDeletionsMode)] = replParams?.PreventDeletionsMode
            };
        }

        public ClusterTopology GetClusterTopology()
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                return _server.GetClusterTopology(ctx);
            }
        }

        protected DatabaseTopology GetTopologyForShard(int shard)
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                return _server.Cluster.ReadDatabaseTopologyForShard(ctx, ShardHelper.ToDatabaseName(_databaseName), shard);
            }
        }

        protected abstract TcpConnectionInfo GetConnectionInfo(ReplicationNode node);

        protected abstract void StartOutgoingReplication(TcpConnectionInfo info, ReplicationNode node);

        protected abstract CancellationToken GetCancellationToken();

        public class ConnectionShutdownInfo
        {
            private readonly TimeSpan _initialTimeout = TimeSpan.FromMilliseconds(1000);
            private readonly int _retriesCount = 0;

            public ConnectionShutdownInfo()
            {
                NextTimeout = _initialTimeout;
                RetriesCount = _retriesCount;
            }

            public string DestinationDbId;

            public long LastHeartbeatTicks;

            public double MaxConnectionTimeout;

            public readonly Queue<Exception> Errors = new Queue<Exception>();

            public TimeSpan NextTimeout { get; set; }

            public DateTime RetryOn { get; set; }

            public ReplicationNode Node { get; set; }

            public int RetriesCount { get; set; }

            public void Reset()
            {
                NextTimeout = _initialTimeout;
                RetriesCount = _retriesCount;
                Errors.Clear();
            }

            public void OnError(Exception e)
            {
                Errors.Enqueue(e);
                while (Errors.Count > 25)
                    Errors.TryDequeue(out _);

                RetriesCount++;
                NextTimeout *= 2;
                NextTimeout = TimeSpan.FromMilliseconds(Math.Min(NextTimeout.TotalMilliseconds, MaxConnectionTimeout));
                RetryOn = DateTime.UtcNow + NextTimeout;
            }
        }
    }
}
