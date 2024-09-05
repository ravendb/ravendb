using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Replication
{
    public abstract class AbstractReplicationLoader<TContextPool, TOperationContext> : IDisposable 
        where TContextPool : JsonContextPoolBase<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();
        private int _replicationStatsId;
        private readonly string _databaseName;
        public readonly TContextPool ContextPool;
        public readonly CancellationToken Token;

        internal readonly ServerStore _server;

        protected readonly RavenLogger _logger;
        protected readonly ConcurrentDictionary<string, IAbstractIncomingReplicationHandler> _incoming = new ConcurrentDictionary<string, IAbstractIncomingReplicationHandler>();

        // PERF: _incoming locks if you do _incoming.Values. Using .Select
        // directly and fetching the Value avoids this problem.
        public IEnumerable<IncomingConnectionInfo> IncomingConnections => _incoming.Select(x => x.Value.ConnectionInfo);

        // PERF: _incoming locks if you do _incoming.Values. Using .Select
        // directly and fetching the Value avoids this problem.
        public IEnumerable<IAbstractIncomingReplicationHandler> IncomingHandlers => _incoming.Select(x => x.Value);
        public string DatabaseName => _databaseName;
        public ServerStore Server => _server;

        protected AbstractReplicationLoader(ServerStore serverStore, string databaseName, TContextPool contextPool, CancellationToken token)
        {
            _databaseName = databaseName;
            ContextPool = contextPool;
            Token = token;
            _server = serverStore;
            _logger = RavenLogManager.Instance.GetLoggerForDatabase(GetType(), databaseName);
        }
        
        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        public sealed class TestingStuff
        {
            public Action<DatabaseOutgoingReplicationHandler> OnOutgoingReplicationStart;
            public Action<Exception> OnIncomingReplicationHandlerFailure;
            public Action OnIncomingReplicationHandlerStart;
            public Action BeforeDisposingIncomingReplicationHandlers;
        }

        public int GetNextReplicationStatsId() => Interlocked.Increment(ref _replicationStatsId);

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
            var getLatestEtagMessage = GetLatestEtagMessage(tcpConnectionOptions, buffer);

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

            return getLatestEtagMessage;
        }

        protected ReplicationLatestEtagRequest GetLatestEtagMessage(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
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
                if (_logger.IsDebugEnabled)
                {
                    _logger.Debug(
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

            if (_logger.IsDebugEnabled)
                _logger.Debug(
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

                    return CertificateLoaderUtil.CreateCertificate(certBytes, sink.CertificatePassword, CertificateLoaderUtil.FlagsForExport);

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

        protected abstract CancellationToken GetCancellationToken();

        public virtual void Dispose()
        {
            _locker.EnterWriteLock();
            try
            {
                foreach (var incoming in _incoming)
                {
                    try
                    {
                        incoming.Value.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }
                }

                _incoming.Clear();
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }
    }
}
