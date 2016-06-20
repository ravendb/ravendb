using System;
using System.Collections.Generic;
using System.Net;

using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStorage
    {
        protected readonly ILog Log = LogManager.GetLogger(typeof(IndexStorage));

        private readonly Index _index;

        private readonly TransactionContextPool _contextPool;

        private readonly TableSchema _errorsSchema = new TableSchema();

        private readonly TableSchema _referencesSchema = new TableSchema();

        private StorageEnvironment _environment;

        public const int MaxNumberOfKeptErrors = 500;

        public IndexStorage(Index index, TransactionContextPool contextPool)
        {
            _index = index;
            _contextPool = contextPool;
        }

        public void Initialize(StorageEnvironment environment)
        {
            _environment = environment;

            CreateSchema();
        }

        private unsafe void CreateSchema()
        {
            _errorsSchema.DefineIndex("ErrorTimestamps", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                IsGlobal = true,
                Name = "ErrorTimestamps"
            });

            _referencesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                IsGlobal = true,
                Name = "PK" // Key,Collection,ReferenceKey
            });

            _referencesSchema.DefineIndex("CollectionAndRefKey", new TableSchema.SchemaIndexDef
            {
                StartIndex = 2,
                Count = 2,
                IsGlobal = true,
                Name = "Collection"
            });

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _errorsSchema.Create(tx.InnerTransaction, "Errors");
                _referencesSchema.Create(tx.InnerTransaction, "References");

                var typeInt = (int)_index.Type;

                var statsTree = tx.InnerTransaction.CreateTree(Schema.StatsTree);
                statsTree.Add(Schema.TypeSlice, Slice.External(context.Allocator, (byte*)&typeInt, sizeof(int)));

                if (statsTree.ReadVersion(Schema.CreatedTimestampSlice) == 0)
                {
                    var binaryDate = SystemTime.UtcNow.ToBinary();
                    statsTree.Add(Schema.CreatedTimestampSlice, Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long)));
                }

                tx.InnerTransaction.CreateTree(Schema.EtagsTree);
                tx.InnerTransaction.CreateTree(Schema.EtagsTombstoneTree);
                tx.InnerTransaction.CreateTree(Schema.EtagsReferenceTree);
                tx.InnerTransaction.CreateTree(Schema.ReferencedCollections);

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
                var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);
                var priorityInt = (int)priority;
                statsTree.Add(Schema.PrioritySlice, Slice.External(context.Allocator, (byte*)&priorityInt, sizeof(int)));

                tx.Commit();
            }
        }

        public IndexingPriority ReadPriority(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);
            var priority = statsTree.Read(Schema.PrioritySlice);
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
                var table = new Table(_errorsSchema, "Errors", tx.InnerTransaction);

                foreach (var sr in table.SeekForwardFrom(_errorsSchema.Indexes["ErrorTimestamps"], Slices.BeforeAllKeys))
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

        public IndexStats ReadStats(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);
            var table = new Table(_errorsSchema, "Errors", tx.InnerTransaction);

            var stats = new IndexStats();
            stats.IsInMemory = _environment.Options is StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions;
            stats.CreatedTimestamp = DateTime.FromBinary(statsTree.Read(Schema.CreatedTimestampSlice).Reader.ReadLittleEndianInt64());
            stats.ErrorsCount = (int)table.NumberOfEntries;

            var lastIndexingTime = statsTree.Read(Schema.LastIndexingTimeSlice);
            if (lastIndexingTime != null)
            {
                stats.LastIndexingTime = DateTime.FromBinary(lastIndexingTime.Reader.ReadLittleEndianInt64());
                stats.MapAttempts = statsTree.Read(Schema.MapAttemptsSlice).Reader.ReadLittleEndianInt32();
                stats.MapErrors = statsTree.Read(Schema.MapErrorsSlice).Reader.ReadLittleEndianInt32();
                stats.MapSuccesses = statsTree.Read(Schema.MapAttemptsSlice).Reader.ReadLittleEndianInt32();

                if (_index.Type.IsMapReduce())
                {
                    stats.ReduceAttempts = statsTree.Read(Schema.ReduceAttemptsSlice).Reader.ReadLittleEndianInt32();
                    stats.ReduceErrors = statsTree.Read(Schema.ReduceErrorsSlice).Reader.ReadLittleEndianInt32();
                    stats.ReduceSuccesses = statsTree.Read(Schema.ReduceSuccessesSlice).Reader.ReadLittleEndianInt32();
                }

                stats.LastIndexedEtags = new Dictionary<string, long>();
                foreach (var collection in _index.Definition.Collections)
                    stats.LastIndexedEtags[collection] = ReadLastIndexedEtag(tx, collection);
            }

            return stats;
        }

        public long ReadLastProcessedTombstoneEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, Schema.EtagsTombstoneTree, Slice.From(tx.InnerTransaction.Allocator, collection));
        }

        public long ReadLastProcessedReferenceEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, Schema.EtagsReferenceTree, Slice.From(tx.InnerTransaction.Allocator, collection));
        }

        public long ReadLastProcessedReferenceTombstoneEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, Schema.EtagsReferenceTombstoneTree, Slice.From(tx.InnerTransaction.Allocator, collection));
        }

        public long ReadLastIndexedEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, Schema.EtagsTree, Slice.From(tx.InnerTransaction.Allocator, collection));
        }

        public void WriteLastReferenceTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, Schema.EtagsReferenceTombstoneTree, Slice.From(tx.InnerTransaction.Allocator, collection), etag);
        }

        public void WriteLastTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, Schema.EtagsTombstoneTree, Slice.From(tx.InnerTransaction.Allocator, collection), etag);
        }

        public void WriteLastReferenceEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, Schema.EtagsReferenceTree, Slice.From(tx.InnerTransaction.Allocator, collection), etag);
        }

        public void WriteLastIndexedEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, Schema.EtagsTree, Slice.From(tx.InnerTransaction.Allocator, collection), etag);
        }

        private unsafe void WriteLastEtag(RavenTransaction tx, string tree, Slice collection, long etag)
        {
            if (Log.IsDebugEnabled)
                Log.Debug($"Writing last etag for '{_index.Name} ({_index.IndexId})'. Tree: {tree}. Collection: {collection}. Etag: {etag}.");

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
            if (Log.IsDebugEnabled)
                Log.Debug($"Updating statistics for '{_index.Name} ({_index.IndexId})'. Stats: {stats}.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = new Table(_errorsSchema, "Errors", tx.InnerTransaction);

                var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);

                statsTree.Increment(Schema.MapAttemptsSlice, stats.MapAttempts);
                statsTree.Increment(Schema.MapSuccessesSlice, stats.MapSuccesses);
                statsTree.Increment(Schema.MapErrorsSlice, stats.MapErrors);

                if (_index.Type.IsMapReduce())
                {
                    statsTree.Increment(Schema.ReduceAttemptsSlice, stats.ReduceAttempts);
                    statsTree.Increment(Schema.ReduceSuccessesSlice, stats.ReduceSuccesses);
                    statsTree.Increment(Schema.ReduceErrorsSlice, stats.ReduceErrors);
                }

                var binaryDate = indexingTime.ToBinary();
                statsTree.Add(Schema.LastIndexingTimeSlice, Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long)));

                if (stats.Errors != null)
                {
                    foreach (var error in stats.Errors)
                    {
                        var ticksBigEndian = IPAddress.HostToNetworkOrder(error.Timestamp.Ticks);
                        var document = context.GetLazyString(error.Document);
                        var action = context.GetLazyString(error.Action);
                        var e = context.GetLazyString(error.Error);

                        var tvb = new TableValueBuilder
                                      {
                                          { (byte*)&ticksBigEndian, sizeof(long) },
                                          { document.Buffer, document.Size },
                                          { action.Buffer, action.Size },
                                          { e.Buffer, e.Size }
                                      };

                        table.Insert(tvb);
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
            table.DeleteForwardFrom(_errorsSchema.Indexes["ErrorTimestamps"], Slices.BeforeAllKeys, numberOfEntriesToDelete);
        }

        public static IndexType ReadIndexType(int indexId, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var statsTree = tx.ReadTree(Schema.StatsTree);
                if (statsTree == null)
                    throw new InvalidOperationException($"Index '{indexId}' does not contain 'Stats' tree.");

                var result = statsTree.Read(Schema.TypeSlice);
                if (result == null)
                    throw new InvalidOperationException($"Stats tree does not contain 'Type' entry in index '{indexId}'.");

                return (IndexType)result.Reader.ReadLittleEndianInt32();
            }
        }

        public IEnumerable<string> GetReferencedCollections(RavenTransaction tx)
        {
            var tree = tx.InnerTransaction.ReadTree(Schema.ReferencedCollections);
            using (var it = tree.Iterate(prefetch: true))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    yield return it.CurrentKey.ToString(); // TODO [ppekrol] ?
                } while (it.MoveNext());
            }
        }

        public IEnumerable<string> GetDocumentKeysFromCollectionThatReference(string collection, LazyStringValue referenceKey, RavenTransaction tx)
        {
            var table = new Table(_referencesSchema, tx.InnerTransaction);

            var seekKey = Slice.From(tx.InnerTransaction.Allocator, collection + referenceKey, ByteStringType.Immutable);
            foreach (var result in table.SeekForwardFrom(_referencesSchema.Indexes["CollectionAndRefKey"], seekKey))
            {
                if (SliceComparer.Equals(seekKey, result.Key) == false)
                    yield break;

                foreach (var tvr in result.Results)
                {
                    yield return GetKeyFromReferenceTableValueReader(tvr, tx).ToString(); // TODO [ppekrol] ?
                }
            }
        }

        private static unsafe Slice GetKeyFromReferenceTableValueReader(TableValueReader tvr, RavenTransaction tx)
        {
            int size;
            return Slice.External(tx.InnerTransaction.Allocator, tvr.Read(1, out size), size);
        }

        public long ReadLastSeenEtagForReference(string collection, LazyStringValue key, RavenTransaction tx)
        {
            var tree = tx.InnerTransaction.ReadTree("$" + collection);

            var value = tree?.Read(key);
            if (value == null)
                return 0;

            return value.Reader.ReadLittleEndianInt64();
        }

        public unsafe void WriteReferences(CurrentIndexingScope indexingScope, RavenTransaction tx)
        {
            if (indexingScope.ReferencedCollections != null)
            {
                var tree = tx.InnerTransaction.ReadTree(Schema.ReferencedCollections);
                foreach (var collection in indexingScope.ReferencedCollections)
                {
                    var collectionSlice = Slice.From(tx.InnerTransaction.Allocator, collection, Sparrow.ByteStringType.Immutable);
                    tree.Add(collectionSlice, Slices.Empty);
                }
            }

            if (indexingScope.ReferencesByCollection != null)
            {
                var table = new Table(_referencesSchema, "References", tx.InnerTransaction);
                foreach (var collections in indexingScope.ReferencesByCollection)
                {
                    var collection = Slice.From(tx.InnerTransaction.Allocator, collections.Key, ByteStringType.Immutable);

                    foreach (var keys in collections.Value)
                    {
                        var key = Slice.From(tx.InnerTransaction.Allocator, keys.Key, ByteStringType.Immutable);

                        foreach (var references in keys.Value)
                        {
                            var referenceKey = Slice.From(tx.InnerTransaction.Allocator, references, ByteStringType.Immutable);
                            var pk = GetReferencePrimaryKey(key, collection, referenceKey, tx.InnerTransaction.Allocator);

                            var read = table.ReadByKey(pk);
                            if (read != null)
                                continue;

                            var tvb = new TableValueBuilder
                            {
                                { pk.Content.Ptr, pk.Size },
                                { key.Content.Ptr, key.Size },
                                { collection.Content.Ptr, collection.Size },
                                { referenceKey.Content.Ptr, referenceKey.Size }
                            };

                            table.Insert(tvb); // duplicates?
                        }
                    }
                }
            }

            if (indexingScope.ReferenceEtagsByCollection != null)
            {
                foreach (var kvp in indexingScope.ReferenceEtagsByCollection)
                {
                    var tree = tx.InnerTransaction.CreateTree("$" + kvp.Key); // $collection
                    foreach (var etags in kvp.Value)
                    {
                        var key = etags.Key;
                        var etag = etags.Value;
                        var keySlice = Slice.From(tx.InnerTransaction.Allocator, key, ByteStringType.Immutable);
                        var etagSlice = Slice.From(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), ByteStringType.Immutable);

                        tree.Add(keySlice, etagSlice);
                    }
                }
            }
        }

        private static unsafe Slice GetReferencePrimaryKey(Slice key, Slice collection, Slice referenceKey, ByteStringContext context)
        {
            var result = context.Allocate(key.Size + collection.Size + referenceKey.Size);
            key.CopyTo(0, result.Ptr, 0, key.Size);
            collection.CopyTo(0, result.Ptr, key.Size, collection.Size);
            referenceKey.CopyTo(0, result.Ptr, key.Size + collection.Size, referenceKey.Size);

            return new Slice(result);
        }

        private class Schema
        {
            public const string StatsTree = "Stats";

            public const string EtagsTree = "Etags";

            public const string EtagsTombstoneTree = "Etags.Tombstone";

            public const string EtagsReferenceTree = "Etags.Reference";

            public const string EtagsReferenceTombstoneTree = "Etags.Reference.Tombstone";

            public const string ReferencedCollections = "Referenced.Collections";

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
        }
    }
}