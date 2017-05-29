using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;
using Voron.Exceptions;

namespace Raven.Server.Documents.Indexes
{
    public class IndexStorage
    {
        protected readonly Logger _logger;

        private readonly Index _index;

        private readonly TransactionContextPool _contextPool;

        public DocumentDatabase DocumentDatabase { get; }

        private readonly TableSchema _errorsSchema = new TableSchema();

        private readonly Dictionary<string, CollectionName> _referencedCollections;

        private StorageEnvironment _environment;

        public const int MaxNumberOfKeptErrors = 500;

        internal bool SimulateCorruption = false;

        public IndexStorage(Index index, TransactionContextPool contextPool, DocumentDatabase database)
        {
            _index = index;
            _contextPool = contextPool;
            DocumentDatabase = database;
            _logger = LoggingSource.Instance.GetLogger<IndexStorage>(DocumentDatabase.Name);

            var referencedCollections = index.GetReferencedCollections();
            if (referencedCollections != null)
                _referencedCollections = referencedCollections
                    .SelectMany(x => x.Value)
                    .Distinct()
                    .ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);
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
                _errorsSchema.Create(tx.InnerTransaction, "Errors", 16);

                var typeInt = (int)_index.Type;

                var statsTree = tx.InnerTransaction.CreateTree(IndexSchema.StatsTree);
                Slice tmpSlice;
                using (Slice.External(context.Allocator, (byte*)&typeInt, sizeof(int), out tmpSlice))
                    statsTree.Add(IndexSchema.TypeSlice, tmpSlice);

                if (statsTree.Read(IndexSchema.CreatedTimestampSlice) == null)
                {
                    var binaryDate = SystemTime.UtcNow.ToBinary();
                    using (Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long), out tmpSlice))
                        statsTree.Add(IndexSchema.CreatedTimestampSlice, tmpSlice);
                }

                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTree);
                tx.InnerTransaction.CreateTree(IndexSchema.EtagsTombstoneTree);
                tx.InnerTransaction.CreateTree(IndexSchema.References);

                _index.Definition.Persist(context, _environment.Options);

                tx.Commit();
            }
        }

        public void WriteDefinition(IndexDefinitionBase indexDefinition)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                indexDefinition.Persist(context, _environment.Options);

                tx.Commit();
            }
        }

        public void WritePriority(IndexPriority priority)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var oldPriority = _index.Definition.Priority;
                try
                {
                    _index.Definition.Priority = priority;
                    _index.Definition.Persist(context, _environment.Options);

                    tx.Commit();
                }
                catch (Exception)
                {
                    _index.Definition.Priority = oldPriority;
                    throw;
                }
            }
        }

        public unsafe void WriteState(IndexState state)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
                var stateInt = (int)state;
                Slice stateSlice;
                using (Slice.External(context.Allocator, (byte*)&stateInt, sizeof(int), out stateSlice))
                    statsTree.Add(IndexSchema.StateSlice, stateSlice);

                tx.Commit();
            }
        }

        public IndexState ReadState(RavenTransaction tx)
        {
            var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);
            var state = statsTree.Read(IndexSchema.StateSlice);
            if (state == null)
                return IndexState.Normal;

            return (IndexState)state.Reader.ReadLittleEndianInt32();
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

                foreach (var tvr in table.SeekForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys, 0))
                {
                    int size;
                    var error = new IndexingError();

                    var ptr = tvr.Result.Reader.Read(0, out size);
                    error.Timestamp = new DateTime(IPAddress.NetworkToHostOrder(*(long*)ptr), DateTimeKind.Utc);

                    ptr = tvr.Result.Reader.Read(1, out size);
                    if(size != 0)
                        error.Document = context.AllocateStringValue(null, ptr, size);

                    ptr = tvr.Result.Reader.Read(2, out size);
                    error.Action = context.AllocateStringValue(null, ptr, size);

                    ptr = tvr.Result.Reader.Read(3, out size);
                    error.Error = context.AllocateStringValue(null, ptr, size);

                    errors.Add(error);
                }
            }

            return errors;
        }

        public long ReadErrorsCount()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");
                return table.NumberOfEntries;
            }
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
            stats.CreatedTimestamp = DateTime.FromBinary(statsTree.Read(IndexSchema.CreatedTimestampSlice).Reader.ReadLittleEndianInt64());
            stats.ErrorsCount = (int)table.NumberOfEntries;

            var lastIndexingTime = statsTree.Read(IndexSchema.LastIndexingTimeSlice);

            stats.Collections = new Dictionary<string, IndexStats.CollectionStats>();
            foreach (var collection in _index.Definition.Collections)
            {
                stats.Collections[collection] = new IndexStats.CollectionStats
                {
                    LastProcessedDocumentEtag = ReadLastIndexedEtag(tx, collection),
                    LastProcessedTombstoneEtag = ReadLastProcessedTombstoneEtag(tx, collection)
                };
            }

            if (lastIndexingTime != null)
            {
                stats.LastIndexingTime = DateTime.FromBinary(lastIndexingTime.Reader.ReadLittleEndianInt64());
                stats.MapAttempts = statsTree.Read(IndexSchema.MapAttemptsSlice).Reader.ReadLittleEndianInt32();
                stats.MapErrors = statsTree.Read(IndexSchema.MapErrorsSlice).Reader.ReadLittleEndianInt32();
                stats.MapSuccesses = statsTree.Read(IndexSchema.MapAttemptsSlice).Reader.ReadLittleEndianInt32();
                stats.MaxNumberOfOutputsPerDocument =
                    statsTree.Read(IndexSchema.MaxNumberOfOutputsPerDocument).Reader.ReadLittleEndianInt32();

                if (_index.Type.IsMapReduce())
                {
                    stats.ReduceAttempts = statsTree.Read(IndexSchema.ReduceAttemptsSlice).Reader.ReadLittleEndianInt32();
                    stats.ReduceErrors = statsTree.Read(IndexSchema.ReduceErrorsSlice).Reader.ReadLittleEndianInt32();
                    stats.ReduceSuccesses = statsTree.Read(IndexSchema.ReduceSuccessesSlice).Reader.ReadLittleEndianInt32();
                }
            }

            return stats;
        }

        public long ReadLastProcessedTombstoneEtag(RavenTransaction tx, string collection)
        {
            Slice collectionSlice;
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out collectionSlice))
            {
                return ReadLastEtag(tx, IndexSchema.EtagsTombstoneTree, collectionSlice);
            }
        }

        public long ReadLastProcessedReferenceEtag(RavenTransaction tx, string collection, CollectionName referencedCollection)
        {
            var tree = tx.InnerTransaction.ReadTree("$" + collection);

            var result = tree?.Read(referencedCollection.Name);
            if (result == null)
                return 0;

            return result.Reader.ReadLittleEndianInt64();
        }

        public long ReadLastProcessedReferenceTombstoneEtag(RavenTransaction tx, string collection, CollectionName referencedCollection)
        {
            var tree = tx.InnerTransaction.ReadTree("%" + collection);

            var result = tree?.Read(referencedCollection.Name);
            if (result == null)
                return 0;

            return result.Reader.ReadLittleEndianInt64();
        }

        public long ReadLastIndexedEtag(RavenTransaction tx, string collection)
        {
            Slice collectionSlice;
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out collectionSlice))
            {
                return ReadLastEtag(tx, IndexSchema.EtagsTree, collectionSlice);
            }
        }

        public unsafe void WriteLastReferenceTombstoneEtag(RavenTransaction tx, string collection, CollectionName referencedCollection, long etag)
        {
            var tree = tx.InnerTransaction.CreateTree("%" + collection);
            Slice collectionSlice;
            Slice etagSlice;
            using (Slice.From(tx.InnerTransaction.Allocator, referencedCollection.Name, ByteStringType.Immutable, out collectionSlice))
            using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
            {
                tree.Add(collectionSlice, etagSlice);
            }
        }

        public void WriteLastTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            Slice collectionSlice;
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out collectionSlice))
            {
                WriteLastEtag(tx, IndexSchema.EtagsTombstoneTree, collectionSlice, etag);
            }
        }

        public unsafe void WriteLastReferenceEtag(RavenTransaction tx, string collection, CollectionName referencedCollection, long etag)
        {
            var tree = tx.InnerTransaction.CreateTree("$" + collection);
            Slice collectionSlice;
            Slice etagSlice;
            using (Slice.From(tx.InnerTransaction.Allocator, referencedCollection.Name, ByteStringType.Immutable, out collectionSlice))
            using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
            {
                tree.Add(collectionSlice, etagSlice);
            }
        }

        public void WriteLastIndexedEtag(RavenTransaction tx, string collection, long etag)
        {
            Slice collectionSlice;
            using (Slice.From(tx.InnerTransaction.Allocator, collection, out collectionSlice))
            {
                WriteLastEtag(tx, IndexSchema.EtagsTree, collectionSlice, etag);
            }
        }

        private unsafe void WriteLastEtag(RavenTransaction tx, string tree, Slice collection, long etag)
        {
            if (SimulateCorruption)
                throw new SimulatedVoronUnrecoverableErrorException("Simulated corruption.");

            if (_logger.IsInfoEnabled)
                _logger.Info($"Writing last etag for '{_index.Name} ({_index.Etag})'. Tree: {tree}. Collection: {collection}. Etag: {etag}.");

            var statsTree = tx.InnerTransaction.CreateTree(tree);
            Slice etagSlice;
            using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
                statsTree.Add(collection, etagSlice);
        }

        public class SimulatedVoronUnrecoverableErrorException : VoronUnrecoverableErrorException
        {
            public SimulatedVoronUnrecoverableErrorException(string message) : base(message)
            {
            }
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

        public unsafe IndexFailureInformation UpdateStats(DateTime indexingTime, IndexingRunStats stats)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Updating statistics for '{_index.Name} ({_index.Etag})'. Stats: {stats}.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var result = new IndexFailureInformation
                {
                    Etag = _index.Etag,
                    Name = _index.Name
                };

                var table = tx.InnerTransaction.OpenTable(_errorsSchema, "Errors");

                var statsTree = tx.InnerTransaction.ReadTree(IndexSchema.StatsTree);

                result.MapAttempts = statsTree.Increment(IndexSchema.MapAttemptsSlice, stats.MapAttempts);
                result.MapSuccesses = statsTree.Increment(IndexSchema.MapSuccessesSlice, stats.MapSuccesses);
                result.MapErrors = statsTree.Increment(IndexSchema.MapErrorsSlice, stats.MapErrors);

                var currentMaxNumberOfOutputs = statsTree.Read(IndexSchema.MaxNumberOfOutputsPerDocument)?.Reader.ReadLittleEndianInt32();

                byte* ptr;
                using (statsTree.DirectAdd(IndexSchema.MaxNumberOfOutputsPerDocument, sizeof(int), out ptr))
                {
                    *(int*)ptr = currentMaxNumberOfOutputs > stats.MaxNumberOfOutputsPerDocument
                        ? currentMaxNumberOfOutputs.Value
                        : stats.MaxNumberOfOutputsPerDocument;
                }

                if (_index.Type.IsMapReduce())
                {
                    result.ReduceAttempts = statsTree.Increment(IndexSchema.ReduceAttemptsSlice, stats.ReduceAttempts);
                    result.ReduceSuccesses = statsTree.Increment(IndexSchema.ReduceSuccessesSlice, stats.ReduceSuccesses);
                    result.ReduceErrors = statsTree.Increment(IndexSchema.ReduceErrorsSlice, stats.ReduceErrors);
                }

                var binaryDate = indexingTime.ToBinary();
                Slice binaryDateslice;
                using (Slice.External(context.Allocator, (byte*)&binaryDate, sizeof(long), out binaryDateslice))
                    statsTree.Add(IndexSchema.LastIndexingTimeSlice, binaryDateslice);

                if (stats.Errors != null)
                {
                    for (var i = Math.Max(stats.Errors.Count - MaxNumberOfKeptErrors, 0); i < stats.Errors.Count; i++)
                    {
                        var error = stats.Errors[i];
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

                return result;
            }
        }

        private void CleanupErrors(Table table)
        {
            if (table.NumberOfEntries <= MaxNumberOfKeptErrors)
                return;

            var numberOfEntriesToDelete = table.NumberOfEntries - MaxNumberOfKeptErrors;
            table.DeleteForwardFrom(_errorsSchema.Indexes[IndexSchema.ErrorTimestampsSlice], Slices.BeforeAllKeys, false, numberOfEntriesToDelete);
        }

        public static IndexType ReadIndexType(long etag, StorageEnvironment environment)
        {
            using (var tx = environment.ReadTransaction())
            {
                var statsTree = tx.ReadTree(IndexSchema.StatsTree);
                if (statsTree == null)
                    throw new InvalidOperationException($"Index '{etag}' does not contain 'Stats' tree.");

                var result = statsTree.Read(IndexSchema.TypeSlice);
                if (result == null)
                    throw new InvalidOperationException($"Stats tree does not contain 'Type' entry in index '{etag}'.");

                return (IndexType)result.Reader.ReadLittleEndianInt32();
            }
        }

        public IEnumerable<Slice> GetDocumentKeysFromCollectionThatReference(string collection, LazyStringValue referenceKey, RavenTransaction tx)
        {
            var collectionTree = tx.InnerTransaction.ReadTree("#" + collection);
            if (collectionTree == null)
                yield break;

            using (DocumentIdWorker.GetLower(tx.InnerTransaction.Allocator, referenceKey, out var k))
            using (var it = collectionTree.MultiRead(k))
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
                        Slice key;
                        using (Slice.From(tx.InnerTransaction.Allocator, keys.Key, ByteStringType.Immutable, out key))
                        {
                            foreach (var referenceKey in keys.Value)
                            {
                                collectionTree.MultiAdd(referenceKey, key);
                                referencesTree.MultiAdd(key, referenceKey);
                            }

                            RemoveReferences(key, collections.Key, keys.Value, tx);
                        }
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
                        CollectionName collectionName;
                        if (_referencedCollections.TryGetValue(collections.Key, out collectionName) == false)
                            throw new InvalidOperationException(
                                $"Could not find collection {collections.Key} in the index storage collections. Should not happen ever!");

                        Slice collectionKey;
                        using (Slice.From(tx.InnerTransaction.Allocator, collectionName.Name, ByteStringType.Immutable,
                                out collectionKey))
                        {
                            var etag = collections.Value;

                            var result = collectionEtagTree.Read(collectionKey);
                            var oldEtag = result?.Reader.ReadLittleEndianInt64();
                            if (oldEtag >= etag)
                                continue;

                            Slice etagSlice;
                            using (Slice.External(tx.InnerTransaction.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
                                collectionEtagTree.Add(collectionKey, etagSlice);
                        }
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
                referenceKey.Release(tx.InnerTransaction.Allocator);
            }
        }

        public void Rename(string name)
        {
            if (_index.Definition.Name == name)
                return;

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _index.Definition.Rename(name, context, _environment.Options);

                tx.Commit();
            }
        }

        private static unsafe ByteStringContext.ExternalScope CreateKey(RavenTransaction tx, LazyStringValue key, out Slice keySlice)
        {
            return Slice.External(tx.InnerTransaction.Allocator, key.Buffer, key.Size, out keySlice);
        }

        private class IndexSchema
        {
            public const string StatsTree = "Stats";

            public const string EtagsTree = "Etags";

            public const string EtagsTombstoneTree = "Etags.Tombstone";

            public const string References = "References";

            public static readonly Slice TypeSlice;

            public static readonly Slice CreatedTimestampSlice;

            public static readonly Slice MapAttemptsSlice;

            public static readonly Slice MapSuccessesSlice;

            public static readonly Slice MapErrorsSlice;

            public static readonly Slice ReduceAttemptsSlice;

            public static readonly Slice ReduceSuccessesSlice;

            public static readonly Slice ReduceErrorsSlice;

            public static readonly Slice LastIndexingTimeSlice;

            public static readonly Slice PrioritySlice;

            public static readonly Slice StateSlice;

            public static readonly Slice ErrorTimestampsSlice;

            public static readonly Slice MaxNumberOfOutputsPerDocument;

            static IndexSchema()
            {
                Slice.From(StorageEnvironment.LabelsContext, "Type", ByteStringType.Immutable, out TypeSlice);
                Slice.From(StorageEnvironment.LabelsContext, "CreatedTimestamp", ByteStringType.Immutable, out CreatedTimestampSlice);
                Slice.From(StorageEnvironment.LabelsContext, "MapAttempts", ByteStringType.Immutable, out MapAttemptsSlice);
                Slice.From(StorageEnvironment.LabelsContext, "MapSuccesses", ByteStringType.Immutable, out MapSuccessesSlice);
                Slice.From(StorageEnvironment.LabelsContext, "MapErrors", ByteStringType.Immutable, out MapErrorsSlice);
                Slice.From(StorageEnvironment.LabelsContext, "ReduceAttempts", ByteStringType.Immutable, out ReduceAttemptsSlice);
                Slice.From(StorageEnvironment.LabelsContext, "ReduceSuccesses", ByteStringType.Immutable, out ReduceSuccessesSlice);
                Slice.From(StorageEnvironment.LabelsContext, "ReduceErrors", ByteStringType.Immutable, out ReduceErrorsSlice);
                Slice.From(StorageEnvironment.LabelsContext, "LastIndexingTime", ByteStringType.Immutable, out LastIndexingTimeSlice);
                Slice.From(StorageEnvironment.LabelsContext, "Priority", ByteStringType.Immutable, out PrioritySlice);
                Slice.From(StorageEnvironment.LabelsContext, "State", ByteStringType.Immutable, out StateSlice);
                Slice.From(StorageEnvironment.LabelsContext, "ErrorTimestamps", ByteStringType.Immutable, out ErrorTimestampsSlice);
                Slice.From(StorageEnvironment.LabelsContext, "MaxNumberOfOutputsPerDocument", ByteStringType.Immutable, out MaxNumberOfOutputsPerDocument);
            }
        }

        public StorageEnvironment Environment()
        {
            return _environment;
        }
    }
}