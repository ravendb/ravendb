﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using Nito.AsyncEx;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public readonly ShardedReplicationContext Replication;

        public class ShardedReplicationContext : AbstractReplicationLoader
        {
            private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

            private readonly ShardedDatabaseContext _context;
            public ShardedDatabaseContext Context => _context;
            public string SourceDatabaseId { get; set; }

            public ShardedReplicationContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore) : base(serverStore, context.DatabaseName)
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
            }

            public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions,
                TcpConnectionHeaderMessage header,
                JsonOperationContext.MemoryBuffer buffer,
                ReplicationQueue replicationQueue)
            {
                var supportedVersions = GetSupportedVersions(tcpConnectionOptions);
                GetReplicationInitialRequest(tcpConnectionOptions, supportedVersions, buffer);

                AssertCanExecute(header);

                CreateIncomingInstance(tcpConnectionOptions, buffer, replicationQueue);
            }

            private void AssertCanExecute(TcpConnectionHeaderMessage header)
            {
                switch (header.AuthorizeInfo?.AuthorizeAs)
                {
                    case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication:
                        throw new InvalidOperationException("Pull Replication is not supported for sharded database");
                    case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication:
                        throw new InvalidOperationException("Push Replication is not supported for sharded database");

                    case null:
                        return;

                    default:
                        throw new InvalidOperationException("Unknown AuthroizeAs value" + header.AuthorizeInfo?.AuthorizeAs);
                }
            }

            protected override CancellationToken GetCancellationToken() => _context.DatabaseShutdown;

            private void CreateIncomingInstance(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer, ReplicationQueue queue)
            {
                ShardedIncomingReplicationHandler newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer, queue);

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

            protected ShardedIncomingReplicationHandler CreateIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions,
                JsonOperationContext.MemoryBuffer buffer, ReplicationQueue replicationQueue)
            {
                var getLatestEtagMessage = IncomingInitialHandshake(tcpConnectionOptions, buffer);

                return new ShardedIncomingReplicationHandler(tcpConnectionOptions, this, buffer, getLatestEtagMessage, replicationQueue);
            }

            protected override TcpConnectionInfo GetConnectionInfo(ReplicationNode node)
            {
                if (node is ShardReplicationNode shardNode == false)
                    throw new InvalidOperationException($"{node} must be of type '{typeof(ShardReplicationNode)}'");

                var shardExecutor = _context.ShardExecutor;
                using (_context.AllocateContext(out JsonOperationContext ctx))
                {
                    var cmd = new GetTcpInfoCommand("sharded-replication", shardNode.Database);
                    RequestExecutor requestExecutor = null;
                    try
                    {
                        requestExecutor = shardExecutor.GetRequestExecutorAt(shardNode.Shard);
                        requestExecutor.Execute(cmd, ctx);
                    }
                    finally
                    {
                        // we want to set node Url even if we fail to connect to destination, so they can be used in replication stats
                        node.Database = shardNode.Database;
                        node.Url = requestExecutor?.Url;
                    }

                    return cmd.Result;
                }
            }

            protected override IAbstractOutgoingReplicationHandler GetOutgoingReplicationHandlerInstance(TcpConnectionInfo info, ReplicationNode node)
            {
                switch (node)
                {
                    case ShardReplicationNode shardNode:
                        return new ShardedOutgoingReplicationHandler(this, shardNode, shardNode.Shard, info, shardNode.ReplicationQueue);

                    default:
                        throw new InvalidOperationException($"{node} must be of type '{typeof(ShardReplicationNode)}'");
                }
            }

            protected override void InvokeOnOutgoingReplicationAdded(IAbstractOutgoingReplicationHandler outgoing)
            {
            }
        }
    }

    public class ReplicationQueue : IDisposable
    {
        public Dictionary<int, BlockingCollection<List<ReplicationBatchItem>>> Items = new Dictionary<int, BlockingCollection<List<ReplicationBatchItem>>>();

        public AsyncCountdownEvent SendToShardCompletion;

        public Dictionary<int, Dictionary<Slice, AttachmentReplicationItem>> AttachmentsPerShard = new Dictionary<int, Dictionary<Slice, AttachmentReplicationItem>>();

        public AsyncManualResetEvent MissingAttachments = new AsyncManualResetEvent(false);

        public string MissingAttachmentMessage;

        public ReplicationQueue(int numberOfShards)
        {
            SendToShardCompletion = new AsyncCountdownEvent(numberOfShards);
            for (int i = 0; i < numberOfShards; i++)
            {
                Items[i] = new BlockingCollection<List<ReplicationBatchItem>>();
                AttachmentsPerShard[i] = new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);
            }
        }

        public void Dispose()
        {
            foreach (var item in Items)
            {
                item.Value?.Dispose();
            }

            foreach (var item in AttachmentsPerShard.Values)
            {
                foreach (var value in item.Values)
                {
                    value.Dispose();
                }
            }

            Items = null;
            AttachmentsPerShard = null;
            SendToShardCompletion = null;
        }
    }

    public class ShardReplicationNode : ExternalReplication
    {
        public int Shard;

        public ReplicationQueue ReplicationQueue;

        public ShardReplicationNode()
        {
        }

        public ShardReplicationNode(string database, string connectionStringName, int shard) : base(database, connectionStringName)
        {
            Shard = shard;
        }
    }
}
