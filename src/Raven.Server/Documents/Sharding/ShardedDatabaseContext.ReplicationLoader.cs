using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public readonly ShardedReplicationContext ReplicationContext;

        public class ShardedReplicationContext : IDisposable
        {
            private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

            private readonly Logger _logger;

            private readonly ShardedDatabaseContext _context;

            private ServerStore _server;

            private readonly ConcurrentDictionary<string, ShardedIncomingReplicationHandler> _incoming =
                new ConcurrentDictionary<string, ShardedIncomingReplicationHandler>();

            private readonly ConcurrentSet<ShardedOutgoingReplicationHandler> _outgoing =
                new ConcurrentSet<ShardedOutgoingReplicationHandler>();

            private int _replicationStatsId;

            public string DatabaseName => _context.DatabaseName;
            public ServerStore Server => _server;
            public ShardedDatabaseContext Context => _context;
            public string SourceDatabaseId { get; set; }
            public DocumentsQueue Queue;

            public ShardedReplicationContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
                _logger = LoggingSource.Instance.GetLogger<ShardedIndexesContext>(context.DatabaseName);
                _server = serverStore;
            }

            public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions,
         TcpConnectionHeaderMessage header,
         X509Certificate2 certificate,
         JsonOperationContext.MemoryBuffer buffer)
            {
                var supportedVersions =
                    TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Replication, tcpConnectionOptions.ProtocolVersion);

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

                string[] allowedPaths = default;
                string pullDefinitionName = null;
                PreventDeletionsMode preventDeletionsMode = PreventDeletionsMode.None;
                switch (header.AuthorizeInfo?.AuthorizeAs)
                {
                    case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication:
                    case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication:
                        if (supportedVersions.Replication.PullReplication == false)
                            throw new InvalidOperationException("Unable to use Pull Replication, because the other side doesn't have it as a supported feature");

                        if (header.AuthorizeInfo.AuthorizationFor == null)
                            throw new InvalidOperationException("Pull replication requires that the AuthorizationFor field will be set, but it wasn't provided");

                        PullReplicationDefinition pullReplicationDefinition;
                        using (_server.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            pullReplicationDefinition = _server.Cluster.ReadPullReplicationDefinition(DatabaseName, header.AuthorizeInfo.AuthorizationFor, ctx);

                            if (pullReplicationDefinition.Disabled)
                                throw new InvalidOperationException("The replication hub " + pullReplicationDefinition.Name + " is disabled and cannot be used currently");
                        }

                        pullDefinitionName = header.AuthorizeInfo.AuthorizationFor;

                        /*switch (header.AuthorizeInfo.AuthorizeAs)
                        {
                            case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication:
                                if (pullReplicationDefinition.Mode.HasFlag(PullReplicationMode.HubToSink) == false)
                                    throw new InvalidOperationException($"Replication hub {header.AuthorizeInfo.AuthorizationFor} does not support Pull Replication");
                                CreatePullReplicationAsHub(tcpConnectionOptions, initialRequest, supportedVersions, pullReplicationDefinition, header);
                                return;

                            case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication:
                                if (pullReplicationDefinition.Mode.HasFlag(PullReplicationMode.SinkToHub) == false)
                                    throw new InvalidOperationException($"Replication hub {header.AuthorizeInfo.AuthorizationFor} does not support Push Replication");
                                if (certificate == null)
                                    throw new InvalidOperationException("Incoming filtered replication is only supported when using a certificate");
#pragma warning disable CS0618 // Type or member is obsolete
                                if (pullReplicationDefinition.Certificates != null && pullReplicationDefinition.Certificates.Count > 0)
#pragma warning restore CS0618 // Type or member is obsolete
                                    throw new InvalidOperationException(
                                        "Incoming filtered replication is not supported on legacy replication hub. Make sure that there are no inline certificates on the replication hub: " +
                                        pullReplicationDefinition.Name);

                                allowedPaths = DetailedReplicationHubAccess.Preferred(header.ReplicationHubAccess.AllowedSinkToHubPaths, header.ReplicationHubAccess.AllowedHubToSinkPaths);
                                preventDeletionsMode = pullReplicationDefinition.PreventDeletionsMode;

                                // same as normal incoming replication, just using the filtering
                                break;

                            default:
                                throw new InvalidOperationException("Unknown AuthroizeAs value: " + header.AuthorizeInfo.AuthorizeAs);
                        }*/
                        break;

                    case null:
                        break;

                    default:
                        throw new InvalidOperationException("Unknown AuthroizeAs value" + header.AuthorizeInfo?.AuthorizeAs);
                }

                ReplicationLoader.PullReplicationParams pullReplicationParams = null;
                if (pullDefinitionName != null)
                {
                    pullReplicationParams = new ReplicationLoader.PullReplicationParams()
                    {
                        Name = pullDefinitionName,
                        AllowedPaths = allowedPaths,
                        Mode = PullReplicationMode.SinkToHub,
                        PreventDeletionsMode = preventDeletionsMode,
                        Type = ReplicationLoader.PullReplicationParams.ConnectionType.Incoming
                    };
                }

                CreateIncomingInstance(tcpConnectionOptions, buffer);
            }


            private void CreateIncomingInstance(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
            {
                var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer);

                //newIncoming.Failed += OnIncomingReceiveFailed;

                // need to safeguard against two concurrent connection attempts
                var current = _incoming.AddOrUpdate(newIncoming.ConnectionInfo.SourceDatabaseId, newIncoming,
                    (_, val) => val.IsDisposed ? newIncoming : val);

                if (current == newIncoming)
                {
                    SourceDatabaseId = newIncoming.ConnectionInfo.SourceDatabaseId;
                    newIncoming.Start();
                }
                else
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info("you can't add two identical connections.", new InvalidOperationException("you can't add two identical connections."));
                    }
                    newIncoming.Dispose();
                }
            }

            protected ShardedIncomingReplicationHandler CreateIncomingReplicationHandler(
                TcpConnectionOptions tcpConnectionOptions,
                JsonOperationContext.MemoryBuffer buffer)
            {
                var getLatestEtagMessage = IncomingInitialHandshake(tcpConnectionOptions, buffer);

                var newIncoming = new ShardedIncomingReplicationHandler(tcpConnectionOptions, this, buffer, getLatestEtagMessage);

                //newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;
                return newIncoming;
            }

            private ReplicationLatestEtagRequest IncomingInitialHandshake(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
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
                    using (_context._serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var writer = new BlittableJsonTextWriter(context, tcpConnectionOptions.Stream))
                    using (context.OpenReadTransaction())
                    {
                        var response = new DynamicJsonValue
                        {
                            [nameof(ReplicationMessageReply.Type)] = nameof(ReplicationMessageReply.ReplyType.Ok),
                            [nameof(ReplicationMessageReply.MessageType)] = ReplicationMessageType.Heartbeat,
                            [nameof(ReplicationMessageReply.NodeTag)] = _server.NodeTag
                        };

                        context.Write(writer, response);
                        writer.Flush();

                        //TODO: get changeVector & last etag from all shards??...
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

            private void AssertValidConnection(IncomingConnectionInfo connectionInfo)
            {
                if (_server.IsPassive())
                {
                    throw new InvalidOperationException(
                        $"Cannot accept the incoming replication connection from {connectionInfo.SourceUrl}, because this node is in passive state.");
                }
            }

            public void AddAndStartOutgoingReplication(ShardReplicationNode node, int shard)
            {
                var info = GetShardedReplicationTcpInfo(node, node.Database, shard);

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
                    var shardedOutgoingReplicationHandler = new ShardedOutgoingReplicationHandler(this, node, shard, info);

                    if (_outgoing.TryAdd(shardedOutgoingReplicationHandler) == false)
                    {
                        return;
                    }

                    shardedOutgoingReplicationHandler.Start();
                }
                finally
                {
                    _locker.ExitReadLock();
                }
               
            }

          private TcpConnectionInfo GetShardedReplicationTcpInfo(ShardReplicationNode exNode, string database, int shard)
            {
                var shardExecutor = _context.ShardExecutor;
                using (_context.AllocateContext(out JsonOperationContext ctx))
                {
                    var cmd = new GetTcpInfoCommand("sharded-replication", database);
                    RequestExecutor requestExecutor = null;
                    try
                    {
                        requestExecutor = shardExecutor.GetRequestExecutorAt(shard);
                        requestExecutor.Execute(cmd, ctx);
                    }
                    finally
                    {
                        // we want to set node Url even if we fail to connect to destination, so they can be used in replication stats
                        exNode.Database = database;
                        exNode.Url = requestExecutor?.Url;
                    }

                    return cmd.Result;
                }
            }

            public X509Certificate2 GetCertificateForReplication(ReplicationNode node, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
            {
                switch (node)
                {
                    case ShardReplicationNode _:
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

            public int GetNextReplicationStatsId()
            {
                return Interlocked.Increment(ref _replicationStatsId);
            }

            public void EnsureNotDeleted(string node)
            {
                using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, DatabaseName))
                    {
                        if (rawRecord != null && rawRecord.DeletionInProgress.ContainsKey(node))
                        {
                            throw new OperationCanceledException($"The database '{DatabaseName}' on node '{node}' is being deleted, so it will not handle replications.");
                        }
                    }
                }
            }

            public void Dispose()
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
                            Console.WriteLine("Failed to dispose _incoming");
                        }
                    }
                    foreach (var outgoing in _outgoing)
                    {
                        try
                        {
                            outgoing.Dispose();
                        }
                        catch
                        {
                            // ignored
                            Console.WriteLine("Failed to dispose _outgoing");
                        }
                    }

                    _outgoing.Clear();
                    _incoming.Clear();
                    
                    Queue.SendToShardCompletion.Dispose();
                    Queue.BatchReadCompletion.Dispose();
                    Queue = null;
                }
                finally
                {
                    _locker.ExitWriteLock();
                }
            }
        }
    }

    public class DocumentsQueue
    {
        public ConcurrentDictionary<int, List<ReplicationBatchItem>> Items = new ConcurrentDictionary<int, List<ReplicationBatchItem>>();

        public ManualResetEvent BatchReadCompletion = new ManualResetEvent(false);

        public CountdownEvent SendToShardCompletion;

        public Dictionary<int, Dictionary<Slice, AttachmentReplicationItem>> AttachmentsPerShard = new Dictionary<int, Dictionary<Slice, AttachmentReplicationItem>>();

        public DocumentsQueue(int numberOfShards)
        {
            SendToShardCompletion = new CountdownEvent(numberOfShards);
            for (int i = 0; i < numberOfShards; i++)
            {
                Items[i] = new List<ReplicationBatchItem>();
                AttachmentsPerShard[i] = new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);
            }
        }
    }

    public class ShardReplicationNode : ExternalReplication
    {
        public int Shard;

        public ShardReplicationNode()
        {
        }

        public ShardReplicationNode(string database, string connectionStringName, int shard) : base(database, connectionStringName)
        {
            Shard = shard;
        }
    }
}
