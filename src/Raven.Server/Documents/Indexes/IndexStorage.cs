using System;
using System.Collections.Generic;
using System.Net;

using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStorage
    {

        protected readonly Logger _logger;

        private readonly Index _index;

        private readonly TransactionContextPool _contextPool;
        public DocumentDatabase DocumentDatabase { get; }

        private readonly TableSchema _errorsSchema = new TableSchema();

        private StorageEnvironment _environment;

        public const int MaxNumberOfKeptErrors = 500;

        public IndexStorage(Index index, TransactionContextPool contextPool, DocumentDatabase database)
        {
            _index = index;
            _contextPool = contextPool;
            DocumentDatabase = database;
            _logger = LoggingSource.Instance.GetLogger<IndexStorage>(DocumentDatabase.Name);
        }

        public void Initialize(StorageEnvironment environment)
        {
            _environment = environment;

            CreateSchema();
        }

        private unsafe void CreateSchema()
        {
            _errorsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                IsGlobal = true,
                Name = IndexSchema.ErrorTimestampsSlice
            });

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _errorsSchema.Create(tx.InnerTransaction, "Errors");

                var typeInt = (int)_index.Type;

                var statsTree = tx.InnerTransaction.CreateTree(IndexSchema.StatsTree);
                statsTree.Add(IndexSchema.TypeSlice, Slice.External(context.Allocator, (byte*)&typeInt, sizeof(int)));

                if (statsTree.ReadVersion(IndexSchema.CreatedTimestampSlice) == 0)
                {
                    var binaryDate = SystemTime.UtcNow.ToBinary();
                    statsTree.Add(IndexSchema.CreatedTimestampSlice, Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long)));
                }

                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTree);
                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTombstoneTree);
                tx.InnerTransaction.CreateTree(IndexSchema.References);

                _index.Definition.Persist(context, _environment.Options);

                tx.Commit();
            }
        }

        public unsafe void WritePriority(IndexingPriority priority)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
                var priorityInt = (int)priority;
                statsTree.Add(IndexSchema.PrioritySlice, Slice.External(context.Allocator, (byte*)&priorityInt, sizeof(int)));

                tx.Commit();
            }
        }

        public IndexingPriority ReadPriority(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
            var priority = statsTree.Read(IndexSchema.PrioritySlice);
            if (priority == null)
                return IndexingPriority.Normal;

            return (IndexingPriority)priority.Reader.ReadLittleEndianInt32();
        }

        public void WriteLock(IndexLockMode mode)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var oldLockMode = _index.Definition.LockMode;
                try
                {
                    _index.Definition.LockMode = mode;
                    _index.Definition.Persist(context, _environment.Options);

                    tx.Commit();
                }
                catch (Exception)
                {
                    _index.Definition.LockMode = oldLockMode;
                    throw;
                }
            }
        }

        public unsafe List<IndexingError> ReadErrors()
        {
            var errors = new List<IndexingError>();

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                foreach (var sr in table.SeekForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys))
                {
                    foreach (var tvr in sr.Results)
                    {
                        int size;
                        var error = new IndexingError();

                        var ptr = tvr.Read(0, out size);
                        error.Timestamp = new DateTime(IPAddress.NetworkToHostOrder(*(long*)ptr), DateTimeKind.Utc);

                        ptr = tvr.Read(1, out size);
                        error.Document = new LazyStringValue(null, ptr, size, context);

                        ptr = tvr.Read(2, out size);
                        error.Action = new LazyStringValue(null, ptr, size, context);

                        ptr = tvr.Read(3, out size);
                        error.Error = new LazyStringValue(null, ptr, size, context);

                        errors.Add(error);
                    }
                }
            }

            return errors;
        }

        public DateTime? ReadLastIndexingTime(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

            var lastIndexingTime = statsTree.Read(IndexSchema.LastIndexingTimeSlice);
            if (lastIndexingTime == null)
                return null;

            return DateTime.FromBinary(lastIndexingTime.Reader.ReadLittleEndianInt64());
        }

        public IndexStats ReadStats(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
            var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

            var stats = new IndexStats();
            stats.IsInMemory = _environment.Options is StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions;
            stats.CreatedTimestamp = DateTime.FromBinary(statsTree.Read(IndexSchema.CreatedTimestampSlice).Reader.ReadLittleEndianInt64());
            stats.ErrorsCount = (int)table.NumberOfEntries;

            var lastIndexingTime = statsTree.Read(IndexSchema.LastIndexingTimeSlice);
            if (lastIndexingTime != null)
            {
                stats.LastIndexingTime = DateTime.FromBinary(lastIndexingTime.Reader.ReadLittleEndianInt64());
                stats.MapAttempts = statsTree.Read(IndexSchema.MapAttemptsSlice).Reader.ReadLittleEndianInt32();
                stats.MapErrors = statsTree.Read(IndexSchema.MapErrorsSlice).Reader.ReadLittleEndianInt32();
                stats.MapSuccesses = statsTree.Read(IndexSchema.MapAttemptsSlice).Reader.ReadLittleEndianInt32();

                if (_index.Type.IsMapReduce())
                {
                    stats.ReduceAttempts = statsTree.Read(IndexSchema.ReduceAttemptsSlice).Reader.ReadLittleEndianInt32();
                    stats.ReduceErrors = statsTree.Read(IndexSchema.ReduceErrorsSlice).Reader.ReadLittleEndianInt32();
                    stats.ReduceSuccesses = statsTree.Read(IndexSchema.ReduceSuccessesSlice).Reader.ReadLittleEndianInt32();
                }

                stats.LastIndexedEtags = new Dictionary<string, long>();
                foreach (var collection in _index.Definition.Collections)
                    stats.LastIndexedEtags[collection] = ReadLastIndexedEtag(tx, collection);
            }

            return stats;
        }

        public long ReadLastProcessedTombstoneEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, IndexSchema.EtagsTombstoneTree, Slice.From(tx.InnerTransaction.Allocator, collection));
        }

        public long ReadLastProcessedReferenceEtag(RavenTransaction tx, string collection, string referencedCollection)
        {
            var tree = tx.InnerTransaction.ReadTree("$" + collection);

            var result = tree?.Read(referencedCollection);
            if (result == null)
                return 0;

            return result.Reader.ReadLittleEndianInt64();
        }

        public long ReadLastProcessedReferenceTombstoneEtag(RavenTransaction tx, string collection, string referencedCollection)
        {
            var tree = tx.InnerTransaction.ReadTree("%" + collection);

            var result = tree?.Read(referencedCollection);
            if (result == null)
                return 0;

            return result.Reader.ReadLittleEndianInt64();
        }

        public long ReadLastIndexedEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, IndexSchema.EtagsTree, Slice.From(tx.InnerTransaction.Allocator, collection));
        }

        public unsafe void WriteLastReferenceTombstoneEtag(RavenTransaction tx, string collection, string referencedCollection, long etag)
        {
            var tree = tx.InnerTransaction.CreateTree("%" + collection);
            var collectionSlice = Slice.From(tx.InnerTransaction.Allocator, referencedCollection, ByteStringType.Immutable);
            var etagSlice = Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long));
            tree.Add(collectionSlice, etagSlice);
        }

        public void WriteLastTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, IndexSchema.EtagsTombstoneTree, Slice.From(tx.InnerTransaction.Allocator, collection), etag);
        }

        public unsafe void WriteLastReferenceEtag(RavenTransaction tx, string collection, string referencedCollection, long etag)
        {
            var tree = tx.InnerTransaction.CreateTree("$" + collection);
            var collectionSlice = Slice.From(tx.InnerTransaction.Allocator, referencedCollection, ByteStringType.Immutable);
            var etagSlice = Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long));
            tree.Add(collectionSlice, etagSlice);
        }

        public void WriteLastIndexedEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, IndexSchema.EtagsTree, Slice.From(tx.InnerTransaction.Allocator, collection), etag);
        }

        private unsafe void WriteLastEtag(RavenTransaction tx, string tree, Slice collection, long etag)
        {
           if (_logger.IsInfoEnabled)
                _logger.Info($"Writing last etag for '{_index.Name} ({_index.IndexId})'. Tree: {tree}. Collection: {collection}. Etag: {etag}.");

            var statsTree = tx.InnerTransaction.CreateTree(tree);
            statsTree.Add(collection, Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long)));
        }

        private static long ReadLastEtag(RavenTransaction tx, string tree, Slice collection)
        {
            var statsTree = tx.InnerTransaction.CreateTree(tree);
            var readResult = statsTree.Read(collection);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            return lastEtag;
        }

        public unsafe void UpdateStats(DateTime indexingTime, IndexingRunStats stats)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Updating statistics for '{_index.Name} ({_index.IndexId})'. Stats: {stats}.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

                statsTree.Increment(IndexSchema.MapAttemptsSlice, stats.MapAttempts);
                statsTree.Increment(IndexSchema.MapSuccessesSlice, stats.MapSuccesses);
                statsTree.Increment(IndexSchema.MapErrorsSlice, stats.MapErrors);

                if (_index.Type.IsMapReduce())
                {
                    statsTree.Increment(IndexSchema.ReduceAttemptsSlice, stats.ReduceAttempts);
                    statsTree.Increment(IndexSchema.ReduceSuccessesSlice, stats.ReduceSuccesses);
                    statsTree.Increment(IndexSchema.ReduceErrorsSlice, stats.ReduceErrors);
                }

                var binaryDate = indexingTime.ToBinary();
                statsTree.Add(IndexSchema.LastIndexingTimeSlice, Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long)));

                if (stats.Errors != null)
                {
                    foreach (var error in stats.Errors)
                    {
                        var ticksBigEndian = Bits.SwapBytes(error.Timestamp.Ticks);
                        using (var document = context.GetLazyString(error.Document))
                        using (var action = context.GetLazyString(error.Action))
                        using (var e = context.GetLazyString(error.Error))
                        {
                            var tvb = new TableValueBuilder
                            {
                                {(byte*) &ticksBigEndian, sizeof (long)},
                                {document.Buffer, document.Size},
                                {action.Buffer, action.Size},
                                {e.Buffer, e.Size}
                            };
                            table.Insert(tvb);
                        }
                    }

                    CleanupErrors(table);
                }

                tx.Commit();
            }
        }

        private void CleanupErrors(Table table)
        {
            if (table.NumberOfEntries <= MaxNumberOfKeptErrors)
                return;

            var numberOfEntriesToDelete = table.NumberOfEntries - MaxNumberOfKeptErrors;
            table.DeleteForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys, numberOfEntriesToDelete);
        }

        public static IndexType ReadIndexType(int indexId, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var statsTree = tx.ReadTree(IndexSchema.StatsTree);
                if (statsTree == null)
                    throw new InvalidOperationException($"Index '{indexId}' does not contain 'Stats' tree.");

                var result = statsTree.Read(IndexSchema.TypeSlice);
                if (result == null)
                    throw new InvalidOperationException($"Stats tree does not contain 'Type' entry in index '{indexId}'.");

                return (IndexType)result.Reader.ReadLittleEndianInt32();
            }
        }

        public IEnumerable<Slice> GetDocumentKeysFromCollectionThatReference(string collection, LazyStringValue referenceKey, RavenTransaction tx)
        {
            var collectionTree = tx.InnerTransaction.ReadTree("#" + collection);
            if (collectionTree == null)
                yield break;

            var referenceKeyAsSlice = CreateKey(tx, referenceKey);
            using (var it = collectionTree.MultiRead(referenceKeyAsSlice))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    yield return it.CurrentKey;
                } while (it.MoveNext());
            }
        }

        public unsafe void WriteReferences(CurrentIndexingScope indexingScope, RavenTransaction tx)
        {
            // IndexSchema:
            // having 'Users' and 'Addresses' we will end up with
            //
            // #Users (tree) - splitted by collection so we can easily return all items of same collection to the indexing function
            // |- addresses/1 (key) -> [ users/1, users/2 ]
            // |- addresses/2 (key) -> [ users/3 ]
            //
            // References (tree) - used in delete operations
            // |- users/1 -> [ addresses/1 ]
            // |- users/2 -> [ addresses/1 ]
            // |- users/3 -> [ addresses/2 ]
            //
            // $Users (tree) - holding highest visible etag of 'referenced collection' per collection, so we will have a starting point for references processing
            // |- Addresses (key) -> 5
            if (indexingScope.ReferencesByCollection != null)
            {
                var referencesTree = tx.InnerTransaction.ReadTree(IndexSchema.References);

                foreach (var collections in indexingScope.ReferencesByCollection)
                {
                    var collectionTree = tx.InnerTransaction.CreateTree("#" + collections.Key); // #collection

                    foreach (var keys in collections.Value)
                    {
                        var key = Slice.From(tx.InnerTransaction.Allocator, keys.Key, ByteStringType.Immutable);

                        foreach (var referenceKey in keys.Value)
                        {
                            collectionTree.MultiAdd(referenceKey, key);
                            referencesTree.MultiAdd(key, referenceKey);
                        }

                        RemoveReferences(key, collections.Key, keys.Value, tx);
                    }
                }
            }

            if (indexingScope.ReferenceEtagsByCollection != null)
            {
                foreach (var kvp in indexingScope.ReferenceEtagsByCollection)
                {
                    var collectionEtagTree = tx.InnerTransaction.CreateTree("$" + kvp.Key); // $collection
                    foreach (var collections in kvp.Value)
                    {
                        var collectionKey = collections.Key;
                        var etag = collections.Value;

                        var result = collectionEtagTree.Read(collectionKey);
                        if (result != null)
                        {
                            var oldEtag = result.Reader.ReadLittleEndianInt64();
                            if (oldEtag >= etag)
                                continue;
                        }

                        var etagSlice = Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long));

                        collectionEtagTree.Add(collectionKey, etagSlice);
                    }
                }
            }
        }

        public void RemoveReferences(Slice key, string collection, HashSet<Slice> referenceKeysToSkip, RavenTransaction tx)
        {
            var referencesTree = tx.InnerTransaction.ReadTree(IndexSchema.References);

            List<Slice> referenceKeys;
            using (var it = referencesTree.MultiRead(key))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return;

                referenceKeys = new List<Slice>();

                do
                {
                    if (referenceKeysToSkip == null || referenceKeysToSkip.Contains(it.CurrentKey) == false)
                        referenceKeys.Add(it.CurrentKey.Clone(tx.InnerTransaction.Allocator, ByteStringType.Immutable));
                } while (it.MoveNext());
            }

            if (referenceKeys.Count == 0)
                return;

            var collectionTree = tx.InnerTransaction.ReadTree("#" + collection);

            foreach (var referenceKey in referenceKeys)
            {
                referencesTree.MultiDelete(key, referenceKey);
                collectionTree?.MultiDelete(referenceKey, key);
            }
        }

        private static unsafe Slice CreateKey(RavenTransaction tx, LazyStringValue key)
        {
            return Slice.External(tx.InnerTransaction.Allocator, key.Buffer, key.Size);
        }

        private class IndexSchema
        {
            public const string StatsTree = "Stats";

            public const string EtagsTree = "Etags";

            public const string EtagsTombstoneTree = "Etags.Tombstone";

            public const string References = "References";

            public static readonly Slice TypeSlice = Slice.From(StorageEnvironment.LabelsContext, "Type", ByteStringType.Immutable);

            public static readonly Slice CreatedTimestampSlice = Slice.From(StorageEnvironment.LabelsContext, "CreatedTimestamp", ByteStringType.Immutable);

            public static readonly Slice MapAttemptsSlice = Slice.From(StorageEnvironment.LabelsContext, "MapAttempts", ByteStringType.Immutable);

            public static readonly Slice MapSuccessesSlice = Slice.From(StorageEnvironment.LabelsContext, "MapSuccesses", ByteStringType.Immutable);

            public static readonly Slice MapErrorsSlice = Slice.From(StorageEnvironment.LabelsContext, "MapErrors", ByteStringType.Immutable);

            public static readonly Slice ReduceAttemptsSlice = Slice.From(StorageEnvironment.LabelsContext, "ReduceAttempts", ByteStringType.Immutable);

            public static readonly Slice ReduceSuccessesSlice = Slice.From(StorageEnvironment.LabelsContext, "ReduceSuccesses", ByteStringType.Immutable);

            public static readonly Slice ReduceErrorsSlice = Slice.From(StorageEnvironment.LabelsContext, "ReduceErrors", ByteStringType.Immutable);

            public static readonly Slice LastIndexingTimeSlice = Slice.From(StorageEnvironment.LabelsContext, "LastIndexingTime", ByteStringType.Immutable);

            public static readonly Slice PrioritySlice = Slice.From(StorageEnvironment.LabelsContext, "Priority", ByteStringType.Immutable);

            public static readonly Slice ErrorTimestampsSlice = Slice.From(StorageEnvironment.LabelsContext, "ErrorTimestamps", ByteStringType.Immutable);
        }

        public StorageEnvironment Environment()
        {
            return _environment;
        }
    }
}