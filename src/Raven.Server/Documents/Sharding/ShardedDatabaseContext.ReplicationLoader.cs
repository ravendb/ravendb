using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
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
        }
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
