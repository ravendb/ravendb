using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Voron;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public readonly ShardedReplicationContext Replication;

        public sealed class ShardedReplicationContext : AbstractReplicationLoader<TransactionContextPool, TransactionOperationContext>
        {
            private readonly ShardedDatabaseContext _context;
            public ShardedDatabaseContext Context => _context;

            public ShardedReplicationContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore) : base(serverStore, context.DatabaseName, serverStore.ContextPool, context.DatabaseShutdown)
            {
                _context = context ?? throw new ArgumentNullException(nameof(context));
            }

            public async Task AcceptIncomingConnectionAsync(TcpConnectionOptions tcpConnectionOptions, TcpConnectionHeaderMessage header, JsonOperationContext.MemoryBuffer buffer)
            {
                var supportedVersions = GetSupportedVersions(tcpConnectionOptions);
                GetReplicationInitialRequest(tcpConnectionOptions, supportedVersions, buffer);

                AssertCanExecute(header);

                await CreateIncomingReplicationHandlerAsync(tcpConnectionOptions, buffer);
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

            private void AddAndStartIncomingInstance(ShardedIncomingReplicationHandler newIncoming)
            {
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

            private async Task CreateIncomingReplicationHandlerAsync(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
            {
                var getLatestEtagMessage = GetLatestEtagMessage(tcpConnectionOptions, buffer);
                var shardedIncomingHandler = new ShardedIncomingReplicationHandler(tcpConnectionOptions, this, buffer, getLatestEtagMessage);

                try
                {
                    using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var writer = new BlittableJsonTextWriter(context, tcpConnectionOptions.Stream))
                    {
                        var (databaseChangeVector, lastEtag) = await shardedIncomingHandler.GetInitialHandshakeResponseFromShardsAsync();

                        var request = base.GetInitialRequestMessage(getLatestEtagMessage);
                        request[nameof(ReplicationMessageReply.DatabaseChangeVector)] = databaseChangeVector;
                        request[nameof(ReplicationMessageReply.LastEtagAccepted)] = lastEtag;

                        context.Write(writer, request);
                        writer.Flush();
                    }

                    AddAndStartIncomingInstance(shardedIncomingHandler);
                }
                catch
                {
                    try
                    {
                        shardedIncomingHandler.Dispose();
                    }
                    catch
                    {
                        // do nothing
                    }
                    try
                    {
                        tcpConnectionOptions.Dispose();
                    }
                    catch
                    {
                        // do nothing
                    }

                    throw;
                }
            }

            public void InvokeOnFailed(ShardedIncomingReplicationHandler handler, Exception e)
            {
                using (handler)
                {
                    _incoming.TryRemove(handler.ConnectionInfo.SourceDatabaseId, out _);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Sharded incoming replication handler has thrown an unhandled exception. ({handler.FromToString})", e);
                }
            }
        }
    }

    public sealed class ReplicationBatch : IDisposable
    {
        public List<ReplicationBatchItem> Items = new();
        public Dictionary<Slice, AttachmentReplicationItem> AttachmentStreams;
        public TaskCompletionSource BatchSent;
        public string LastAcceptedChangeVector;
        public long LastEtagAccepted;
        public long LastSentEtagFromSource;

        public void Dispose()
        {
            BatchSent?.TrySetCanceled();

            if (AttachmentStreams != null)
            {
                foreach (var attachment in AttachmentStreams)
                    attachment.Value?.Dispose();

                AttachmentStreams.Clear();
            }

            foreach (var item in Items)
            {
                item.Dispose();
            }

            Items.Clear();
            BatchSent = null;
        }
    }

    public sealed class ShardReplicationNode : ExternalReplication
    {
        public int ShardNumber;

        public ShardReplicationNode()
        {
        }

        public ShardReplicationNode(string database, string connectionStringName, int shardNumber) : base(database, connectionStringName)
        {
            ShardNumber = shardNumber;
        }
    }
}
