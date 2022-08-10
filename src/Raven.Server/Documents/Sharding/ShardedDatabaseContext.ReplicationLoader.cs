using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using NCrontab.Advanced.Extensions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public readonly ShardedReplicationContext Replication;

        public class ShardedReplicationContext : AbstractReplicationLoader
        {
            private readonly ShardedDatabaseContext _context;
            public ShardedDatabaseContext Context => _context;

            public ShardedReplicationContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore) : base(serverStore, context.DatabaseName)
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
            }

            public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions,
                TcpConnectionHeaderMessage header,
                JsonOperationContext.MemoryBuffer buffer)
            {
                var supportedVersions = GetSupportedVersions(tcpConnectionOptions);
                GetReplicationInitialRequest(tcpConnectionOptions, supportedVersions, buffer);

                AssertCanExecute(header);

                CreateIncomingInstance(tcpConnectionOptions, buffer);
            }

            private void AssertCanExecute(TcpConnectionHeaderMessage header)
            {
                switch (header.AuthorizeInfo?.AuthorizeAs)
                {
                    case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication:
                        throw new NotSupportedInShardingException("Pull Replication is not supported for sharded database");
                    case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication:
                        throw new NotSupportedInShardingException("Push Replication is not supported for sharded database");

                    case null:
                        return;

                    default:
                        throw new InvalidOperationException("Unknown AuthroizeAs value" + header.AuthorizeInfo?.AuthorizeAs);
                }
            }

            protected override CancellationToken GetCancellationToken() => _context.DatabaseShutdown;

            private void CreateIncomingInstance(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
            {
                ShardedIncomingReplicationHandler newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer);

                var current = _incoming.AddOrUpdate(newIncoming.ConnectionInfo.SourceDatabaseId, newIncoming,
                    (_, val) => val.IsDisposed ? newIncoming : val);

                if (current == newIncoming)
                {
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

            protected ShardedIncomingReplicationHandler CreateIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions,
                JsonOperationContext.MemoryBuffer buffer)
            {
                var getLatestEtagMessage = IncomingInitialHandshake(tcpConnectionOptions, buffer);

                return new ShardedIncomingReplicationHandler(tcpConnectionOptions, this, buffer, getLatestEtagMessage);
            }

            protected override DynamicJsonValue GetInitialRequestMessage(ReplicationLatestEtagRequest replicationLatestEtagRequest,
                ReplicationLoader.PullReplicationParams replParams = null)
            {
                var (databaseChangeVector, lastReplicateEtag) = GetShardsLastReplicateInfo(replicationLatestEtagRequest.SourceDatabaseId);
               
                var request = base.GetInitialRequestMessage(replicationLatestEtagRequest, replParams);
                request[nameof(ReplicationMessageReply.DatabaseChangeVector)] = databaseChangeVector;
                request[nameof(ReplicationMessageReply.LastEtagAccepted)] = lastReplicateEtag;
                return request;
            }

            private (string LastAcceptedChangeVector, long LastEtagFromSource) GetShardsLastReplicateInfo(string sourceDatabaseId)
            {
                var shardExecutor = _context.ShardExecutor;

                var shardsChangeVector = new List<string>();
                long lastEtagFromSource = 0;
                using (_context.AllocateContext(out JsonOperationContext ctx))
                {
                    for (int i = 0; i < _context.ShardCount; i++)
                    {
                        var shardDatabaseName = ShardHelper.ToShardName(DatabaseName, i);

                        var cmd = new GetShardDatabaseReplicationInfoCommand(sourceDatabaseId, shardDatabaseName);
                        var requestExecutor = shardExecutor.GetRequestExecutorAt(i);
                        requestExecutor.Execute(cmd, ctx);
                        var replicationInfo = cmd.Result;

                        var changeVector = replicationInfo.DatabaseChangeVector;
                        if (changeVector.IsNullOrWhiteSpace() == false)
                            shardsChangeVector.Add(changeVector);

                        var lastReplicateEtag = replicationInfo.LastEtagFromSource;
                        lastEtagFromSource = Math.Max(lastEtagFromSource, lastReplicateEtag);
                    }
                }
                
                var mergedChangeVector = ChangeVectorUtils.MergeVectors(shardsChangeVector);
                return (mergedChangeVector, lastEtagFromSource);
            }
        }
    }

    internal class GetShardDatabaseReplicationInfoCommand : RavenCommand<ReplicationInitialInfo>
    {
        private readonly string _sourceDatabaseId;
        private readonly string _dbName;

        public GetShardDatabaseReplicationInfoCommand(string sourceDatabaseId, string dbName)
        {
            _sourceDatabaseId = sourceDatabaseId;
            _dbName = dbName;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{_dbName}/replication/initialReplicationInfo?sourceId={_sourceDatabaseId}";

            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationServer.ReplicationInitialInfo(response);
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;
    }

    public class ReplicationInitialInfo
    {
        public string DatabaseChangeVector;
        public long LastEtagFromSource;
    }

    public class ReplicationBatch
    {
        public List<ReplicationBatchItem> Items = new();
        public Dictionary<Slice, AttachmentReplicationItem> Attachments;
        public TaskCompletionSource BatchSent;
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
