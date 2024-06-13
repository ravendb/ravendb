using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Data;
using Voron;
using Voron.Impl;

namespace Raven.Server.Rachis;

public partial class Follower
{
    public sealed class FollowerReadAndCommitSnapshotCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly RachisConsensus _engine;
        private readonly Follower _follower;
        private readonly InstallSnapshot _snapshot;
        private readonly CancellationToken _token;

        public Task OnFullSnapshotInstalledTask { get; private set; }

        public FollowerReadAndCommitSnapshotCommand([NotNull] RachisConsensus engine, Follower follower, [NotNull] InstallSnapshot snapshot, CancellationToken token)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _follower = follower;
            _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
            _token = token;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            var lastTerm = _engine.GetTermFor(context, _snapshot.LastIncludedIndex);
            var lastCommitIndex = _engine.GetLastEntryIndex(context);

            if (_engine.GetSnapshotRequest(context) == false &&
                _snapshot.LastIncludedTerm == lastTerm && _snapshot.LastIncludedIndex < lastCommitIndex)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info(
                        $"{ToString()}: Got installed snapshot with last index={_snapshot.LastIncludedIndex} while our lastCommitIndex={lastCommitIndex}, will just ignore it");
                }

                //This is okay to ignore because we will just get the committed entries again and skip them
                ReadInstallSnapshotAndIgnoreContent(_token);
            }
            else if (InstallSnapshot(context, _token))
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info(
                        $"{ToString()}: Installed snapshot with last index={_snapshot.LastIncludedIndex} with LastIncludedTerm={_snapshot.LastIncludedTerm} ");
                }

                _engine.SetLastCommitIndex(context, _snapshot.LastIncludedIndex, _snapshot.LastIncludedTerm);
                _engine.ClearLogEntriesAndSetLastTruncate(context, _snapshot.LastIncludedIndex, _snapshot.LastIncludedTerm);

                OnFullSnapshotInstalledTask = _engine.OnSnapshotInstalled(context, _snapshot.LastIncludedIndex, _token);
            }
            else
            {
                var lastEntryIndex = _engine.GetLastEntryIndex(context);
                if (lastEntryIndex < _snapshot.LastIncludedIndex)
                {
                    var message =
                        $"The snapshot installation had failed because the last included index {_snapshot.LastIncludedIndex} in term {_snapshot.LastIncludedTerm} doesn't match the last entry {lastEntryIndex}";
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: {message}");
                    }

                    throw new InvalidOperationException(message);
                }
            }

            // snapshot always has the latest topology
            if (_snapshot.Topology == null)
            {
                const string message = "Expected to get topology on snapshot";
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: {message}");
                }

                throw new InvalidOperationException(message);
            }

            using (var topologyJson = context.ReadObject(_snapshot.Topology, "topology"))
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: topology on install snapshot: {topologyJson}");
                }

                var topology = JsonDeserializationRachis<ClusterTopology>.Deserialize(topologyJson);

                RachisConsensus.SetTopology(_engine, context, topology);
            }

            _engine.SetSnapshotRequest(context, false);

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += t =>
            {
                if (t is LowLevelTransaction llt && llt.Committed)
                {
                    // we might have moved from passive node, so we need to start the timeout clock
                    _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);
                }
            };

            return 1;
        }

        private bool InstallSnapshot(ClusterOperationContext context, CancellationToken token)
        {
            var txw = context.Transaction.InnerTransaction;

            var fileName = $"snapshot.{Guid.NewGuid():N}";
            var filePath = context.Environment.Options.TempPath.Combine(fileName);

            using (var temp = new StreamsTempFile(filePath.FullPath, context.Environment))
            using (var stream = temp.StartNewStream())
            using (var remoteReader = _follower._connection.CreateReaderToStream(stream))
            {
                if (ReadSnapshot(remoteReader, context, txw, dryRun: true, token) == false)
                    return false;

                stream.Seek(0, SeekOrigin.Begin);
                using (var fileReader = new StreamSnapshotReader(stream))
                {
                    ReadSnapshot(fileReader, context, txw, dryRun: false, token);
                }
            }

            return true;
        }

        private unsafe bool ReadSnapshot(SnapshotReader reader, ClusterOperationContext context, Transaction txw, bool dryRun, CancellationToken token)
        {
            var type = reader.ReadInt32();
            if (type == -1)
                return false;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                int size;
                long entries;
                switch ((RootObjectType)type)
                {
                    case RootObjectType.None:
                        return true;
                    case RootObjectType.VariableSizeTree:
                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        Tree tree = null;
                        Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice treeName); // The Slice will be freed on context close

                        entries = reader.ReadInt64();
                        var flags = TreeFlags.FixedSizeTrees;

                        if (dryRun == false)
                        {
                            txw.DeleteTree(treeName);
                            tree = txw.CreateTree(treeName);
                        }

                        if (_follower._connection.Features.MultiTree)
                            flags = (TreeFlags)reader.ReadInt32();

                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            // read key
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);

                            using (Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice valKey))
                            {
                                switch (flags)
                                {
                                    case TreeFlags.None:

                                        // this is a very specific code to block receiving 'CompareExchangeByExpiration' which is a multi-value tree
                                        // while here we expect a normal tree
                                        if (SliceComparer.Equals(valKey, CompareExchangeExpirationStorage.CompareExchangeByExpiration))
                                            throw new InvalidOperationException($"{valKey} is a multi-tree, please upgrade the leader node.");

                                        // read value
                                        size = reader.ReadInt32();
                                        reader.ReadExactly(size);

                                        if (dryRun == false)
                                        {
                                            using (tree.DirectAdd(valKey, size, out byte* ptr))
                                            {
                                                fixed (byte* pBuffer = reader.Buffer)
                                                {
                                                    Memory.Copy(ptr, pBuffer, size);
                                                }
                                            }
                                        }

                                        break;
                                    case TreeFlags.MultiValueTrees:
                                        var multiEntries = reader.ReadInt64();
                                        for (int j = 0; j < multiEntries; j++)
                                        {
                                            token.ThrowIfCancellationRequested();

                                            size = reader.ReadInt32();
                                            reader.ReadExactly(size);

                                            if (dryRun == false)
                                            {
                                                using (Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice multiVal))
                                                {
                                                    tree.MultiAdd(valKey, multiVal);
                                                }
                                            }
                                        }

                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException($"Got unkonwn type '{type}'");
                                }
                            }
                        }

                        break;
                    case RootObjectType.Table:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        TableValueReader tvr;
                        Table table = null;
                        if (dryRun == false)
                        {
                            Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable,
                                out Slice tableName); //The Slice will be freed on context close
                            var tableTree = txw.ReadTree(tableName, RootObjectType.Table);

                            // Get the table schema
                            var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                            var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
                            if (schemaPtr == null)
                                throw new InvalidOperationException(
                                    "When trying to install snapshot, found missing table " + tableName);

                            var schema = TableSchema.ReadFrom(txw.Allocator, schemaPtr, schemaSize);

                            table = txw.OpenTable(schema, tableName);

                            // delete the table
                            while (true)
                            {
                                token.ThrowIfCancellationRequested();
                                if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out tvr) == false)
                                    break;
                                table.Delete(tvr.Id);
                            }
                        }

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);

                            if (dryRun == false)
                            {
                                fixed (byte* pBuffer = reader.Buffer)
                                {
                                    tvr = new TableValueReader(pBuffer, size);
                                    table.Insert(ref tvr);
                                }
                            }
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }

                type = reader.ReadInt32();
            }
        }

        private void ReadInstallSnapshotAndIgnoreContent(CancellationToken token)
        {
            var reader = _follower._connection.CreateReader();
            while (true)
            {
                token.ThrowIfCancellationRequested();

                var type = reader.ReadInt32();
                if (type == -1)
                    return;

                int size;
                long entries;
                switch ((RootObjectType)type)
                {
                    case RootObjectType.None:
                        return;
                    case RootObjectType.VariableSizeTree:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                        }

                        break;
                    case RootObjectType.Table:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }
            }
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(
            ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }

    }

}
