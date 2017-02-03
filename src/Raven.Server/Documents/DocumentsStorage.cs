using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Versioning;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;
using Voron.Data;
using Voron.Util;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private static readonly Slice KeySlice;

        private static readonly Slice DocsSlice;
        private static readonly Slice CollectionEtagsSlice;
        private static readonly Slice AllDocsEtagsSlice;
        private static readonly Slice TombstonesSlice;
        private static readonly Slice KeyAndChangeVectorSlice;
        private static readonly Slice AllConflictedDocsEtags;
        private static readonly Slice ConflictedCollection;
       
        public static readonly TableSchema DocsSchema = new TableSchema();
        private static readonly Slice TombstonesPrefix;
        private static readonly Slice DeletedEtagsSlice;
        private static readonly TableSchema ConflictsSchema = new TableSchema();
        private static readonly TableSchema TombstonesSchema = new TableSchema();
        private static readonly TableSchema CollectionsSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;

        private Dictionary<string, CollectionName> _collectionsCache;

        public int NextPage;
        public enum ConflictsTable
        {
            LoweredKey = 0,
            ChangeVector = 1,
            OriginalKey = 2,
            Data = 3,
            Etag = 4,
            Collection = 5,
            LastModified = 6,
        }
        static DocumentsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "AllTombstonesEtags", ByteStringType.Immutable, out AllTombstonesEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastEtag", ByteStringType.Immutable, out LastEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Key", ByteStringType.Immutable, out KeySlice);
            Slice.From(StorageEnvironment.LabelsContext, "Docs", ByteStringType.Immutable, out DocsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllConflictedDocsEtags", ByteStringType.Immutable, out AllConflictedDocsEtags);
            Slice.From(StorageEnvironment.LabelsContext, "Tombstones", ByteStringType.Immutable, out TombstonesSlice);
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndChangeVector", ByteStringType.Immutable, out KeyAndChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, CollectionName.GetTablePrefix(CollectionTableType.Tombstones), ByteStringType.Immutable, out TombstonesPrefix);
            Slice.From(StorageEnvironment.LabelsContext, "DeletedEtags", ByteStringType.Immutable, out DeletedEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "ConflictedCollection", ByteStringType.Immutable, out ConflictedCollection);
      
            /*
            Collection schema is:
            full name
            collections are never deleted from the collections table
            */
            CollectionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = false,
            });

            /*
             The structure of conflicts table starts with the following fields:
             [ Conflicted Doc Id | Change Vector | ... the rest of fields ... ]
             PK of the conflicts table will be 'Change Vector' field, because when dealing with conflicts,
              the change vectors will always be different, hence the uniqueness of the key. (inserts/updates will not overwrite)

            Additional indice is set to have composite key of 'Conflicted Doc Id' and 'Change Vector' so we will be able to iterate
            on conflicts by conflicted doc id (using 'starts with')
             */

            ConflictsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.ChangeVector,
                Count = 1,
                IsGlobal = false,
                Name = KeySlice
            });

            // required to get conflicts by key
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.LoweredKey,
                Count = 2,
                IsGlobal = false,
                Name = KeyAndChangeVectorSlice
            });

            ConflictsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.Etag,
                IsGlobal = true,
                Name = AllConflictedDocsEtags
            });

            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.Collection,
                Count = 1,
                IsGlobal = true,
                Name = ConflictedCollection
            });
            
            // The documents schema is as follows
            // fields (lowered key, etag, lazy string key, document, change vector, last modified, optional flags)
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            DocsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = DocsSlice
            });

            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false,
                Name = CollectionEtagsSlice
            });

            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true,
                Name = AllDocsEtagsSlice
            });

            TombstonesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = TombstonesSlice
            });

            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false,
                Name = CollectionEtagsSlice
            });

            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true,
                Name = AllTombstonesEtagsSlice
            });

            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef()
            {
                StartIndex = 2,
                IsGlobal = false,
                Name = DeletedEtagsSlice
            });
        }

        private readonly Logger _logger;
        private readonly string _name;
        private static readonly Slice AllTombstonesEtagsSlice;
        private static readonly Slice LastEtagSlice;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        public string DataDirectory;
        public DocumentsContextPool ContextPool;
        private UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        private long _hasConflicts;

        public DocumentsStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _name = _documentDatabase.Name;
            _logger = LoggingSource.Instance.GetLogger<DocumentsStorage>(documentDatabase.Name);
        }

        public StorageEnvironment Environment { get; private set; }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentsStorage)}");

            exceptionAggregator.Execute(() =>
            {
                _unmanagedBuffersPool?.Dispose();
                _unmanagedBuffersPool = null;
            });

            exceptionAggregator.Execute(() =>
            {
                ContextPool?.Dispose();
                ContextPool = null;
            });

            exceptionAggregator.Execute(() =>
            {
                Environment?.Dispose();
                Environment = null;
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        public void Initialize()
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info
                    ("Starting to open document storage for " + (_documentDatabase.Configuration.Core.RunInMemory ?
                    "<memory>" : _documentDatabase.Configuration.Core.DataDirectory.FullPath));
            }

            var options = _documentDatabase.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(_documentDatabase.Configuration.Core.DataDirectory.FullPath)
                : StorageEnvironmentOptions.ForPath(
                    _documentDatabase.Configuration.Core.DataDirectory.FullPath,
                    _documentDatabase.Configuration.Storage.TempPath?.FullPath,
                    _documentDatabase.Configuration.Storage.JournalsStoragePath?.FullPath
                    );

            try
            {
                Initialize(options);
            }
            catch (Exception)
            {
                options.Dispose();
                throw;
            }
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            options.SchemaVersion = 1;
            try
            {
                Environment = new StorageEnvironment(options);
                ContextPool = new DocumentsContextPool(_documentDatabase);

                using (var tx = Environment.WriteTransaction())
                {
                    NewPageAllocator.MaybePrefetchSections(
                        tx.LowLevelTransaction.RootObjects,
                        tx.LowLevelTransaction);

                    tx.CreateTree("Docs");
                    tx.CreateTree("LastReplicatedEtags");
                    tx.CreateTree("Identities");
                    tx.CreateTree("ChangeVector");

                    ConflictsSchema.Create(tx, "Conflicts", 32);
                    CollectionsSchema.Create(tx, "Collections", 32);

                    _hasConflicts = tx.OpenTable(ConflictsSchema, "Conflicts").NumberOfEntries;

                    _lastEtag = ReadLastEtag(tx);
                    _collectionsCache = ReadCollections(tx);

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not open server store for " + _name, e);

                options.Dispose();
                Dispose();
                throw;
            }
        }

        private static void AssertTransaction(DocumentsOperationContext context)
        {
            if (context.Transaction == null) //precaution
                throw new InvalidOperationException("No active transaction found in the context, and at least read transaction is needed");
        }

        public ChangeVectorEntry[] GetDatabaseChangeVector(DocumentsOperationContext context)
        {
            AssertTransaction(context);

            var tree = context.Transaction.InnerTransaction.ReadTree("ChangeVector");
            return ReplicationUtils.ReadChangeVectorFrom(tree);
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, Dictionary<Guid, long> changeVector)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree("ChangeVector");
            ReplicationUtils.WriteChangeVectorTo(context, changeVector, tree);
        }

        public static long ReadLastDocumentEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, AllDocsEtagsSlice);
        }

        public static long ReadLastTombstoneEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, AllTombstonesEtagsSlice);
        }

        public static long ReadLastCoflictsEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, AllConflictedDocsEtags);
        }

        public static long ReadLastRevisionsEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, VersioningStorage.RevisionsEtags);
        }

        private static long ReadLastEtagFrom(Transaction tx, Slice name)
        {
            using (var fst = new FixedSizeTree(tx.LowLevelTransaction,
                tx.LowLevelTransaction.RootObjects,
                name, sizeof(long),
                clone: false))
            {
                using (var it = fst.Iterate())
                {
                    if (it.SeekToLast())
                        return it.CurrentKey;
                }
            }

            return 0;
        }

        public static long ReadLastEtag(Transaction tx)
        {
            var tree = tx.CreateTree("Etags");
            var readResult = tree.Read(LastEtagSlice);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            var lastDocumentEtag = ReadLastDocumentEtag(tx);
            if (lastDocumentEtag > lastEtag)
                lastEtag = lastDocumentEtag;

            var lastTombstoneEtag = ReadLastTombstoneEtag(tx);
            if (lastTombstoneEtag > lastEtag)
                lastEtag = lastTombstoneEtag;

            var lastConflictEtag = ReadLastCoflictsEtag(tx);
            if (lastConflictEtag > lastEtag)
                lastEtag = lastConflictEtag;

            var lastRevisionsEtag = ReadLastRevisionsEtag(tx);
            if (lastRevisionsEtag > lastEtag)
                lastEtag = lastRevisionsEtag;

            return lastEtag;
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string prefix, string matches, string exclude, int start, int take)
        {
            int docCount = 0;
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            var originalTake = take;
            var originalstart = start;

            Slice prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, prefix, out prefixSlice))
            {
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var result in table.SeekByPrimaryKey(prefixSlice, startsWith: true))
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    docCount++;
                    var document = TableValueToDocument(context, ref result.Reader);
                    string documentKey = document.Key;
                    if (documentKey.StartsWith(prefix, StringComparison.CurrentCultureIgnoreCase) == false)
                        break;

                    var keyTest = documentKey.Substring(prefix.Length);
                    if (!WildcardMatcher.Matches(matches, keyTest) ||
                        WildcardMatcher.MatchesExclusion(exclude, keyTest))
                        continue;

                    if (take-- <= 0)
                    {
                        if (docCount >= originalTake)
                            NextPage = (originalstart + docCount - 1);
                        else
                            NextPage = (originalstart);

                        yield break;
                    }
                    yield return document;
                }
            }

            if (docCount >= originalTake)
                NextPage = (originalstart + docCount);
            else
                NextPage = (originalstart);
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], long.MaxValue))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, string collection, int start, int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents),
                throwIfDoesNotExist: false);

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, long etag, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                {
                    yield break;
                }

                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag))
            {
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<Document> GetDocuments(DocumentsOperationContext context, List<Slice> ids, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            foreach (var id in ids)
            {
                // id must be lowercased

                TableValueReader reader;
                if (table.ReadByKey(id, out reader) == false)
                    continue;

                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;

                yield return TableValueToDocument(context, ref reader);
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, string collection, long etag, int start, int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents),
                throwIfDoesNotExist: false);

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], etag))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public List<DocumentConflict> GetAllConflictsBySameKeyAfter(DocumentsOperationContext context, ref Slice lastKey)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");
            var list = new List<DocumentConflict>();
            LazyStringValue firstKey = null;
            foreach (var tvrs in table.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], lastKey))
            {
                var tvr = tvrs.Results.Single();
                var conflict = TableValueToConflictDocument(context, ref tvr.Reader);
                if (lastKey.Size == conflict.LoweredKey.Length &&
                    Memory.Compare(lastKey.Content.Ptr, conflict.LoweredKey.Buffer, lastKey.Size) == 0)
                {
                    // same key as we already seen, skip it
                    break;
                }
                if (firstKey == null)
                    firstKey = conflict.LoweredKey;
                list.Add(conflict);

                if (firstKey.Equals(conflict.LoweredKey) == false)
                    break;
            }
            if (list.Count > 0)
            {
                lastKey.Release(context.Allocator);
                // we have to clone this, because it might be removed by the time we come back here
                Slice.From(context.Allocator, list[0].LoweredKey.Buffer, list[0].LoweredKey.Size, out lastKey);
            }
            return list;
        }

        public IEnumerable<DocumentConflict> GetConflictsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtags], etag))
            {
                yield return TableValueToConflictDocument(context, ref tvr.Reader);
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, List<string> collections, long etag, int take)
        {
            foreach (var collection in collections)
            {
                if (take <= 0)
                    yield break;

                foreach (var document in GetDocumentsFrom(context, collection, etag, 0, int.MaxValue))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return document;
                }
            }
        }

        public Tuple<Document, DocumentTombstone> GetDocumentOrTombstone(DocumentsOperationContext context, string key, bool throwOnConflict = true)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            Slice loweredKey;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out loweredKey))
            {
                return GetDocumentOrTombstone(context, loweredKey, throwOnConflict);
            }
        }

        public Tuple<Document, DocumentTombstone> GetDocumentOrTombstone(DocumentsOperationContext context, Slice loweredKey, bool throwOnConflict = true)
        {
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            try
            {
                var doc = Get(context, loweredKey);
                if (doc != null)
                    return Tuple.Create<Document, DocumentTombstone>(doc, null);
            }
            catch (DocumentConflictException)
            {
                if (throwOnConflict)
                    throw;
            }

            var tombstoneTable = new Table(TombstonesSchema, context.Transaction.InnerTransaction);
            TableValueReader tvr;
            tombstoneTable.ReadByKey(loweredKey, out tvr);

            return Tuple.Create<Document, DocumentTombstone>(null, TableValueToTombstone(context, ref tvr));
        }

        public Document Get(DocumentsOperationContext context, string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            Slice loweredKey;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out loweredKey))
            {
                return Get(context, loweredKey);
            }
        }

        public Document Get(DocumentsOperationContext context, Slice loweredKey)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            TableValueReader tvr;
            if (table.ReadByKey(loweredKey, out tvr) == false)
            {
                if (_hasConflicts != 0)
                    ThrowDocumentConflictIfNeeded(context, loweredKey);
                return null;
            }

            var doc = TableValueToDocument(context, ref tvr);

            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(doc);

            return doc;
        }

        public bool HasMoreOfTombstonesAfter(
            DocumentsOperationContext context,
            long etag,
            int maxAllowed)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (
                var _ in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag))
            {
                if (maxAllowed-- < 0)
                    return true;
            }
            return false;
        }

        public IEnumerable<DocumentTombstone> GetTombstonesFrom(
            DocumentsOperationContext context,
            long etag,
            int start,
            int take)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }

                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public IEnumerable<DocumentTombstone> GetTombstonesFrom(
            DocumentsOperationContext context,
            string collection,
            long etag,
            int start,
            int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones),
                throwIfDoesNotExist: false);

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public long GetLastDocumentEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents),
                throwIfDoesNotExist: false
                );

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table
                        .SeekBackwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue)
                        .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Reader.Read(1, out size);
            return IPAddress.NetworkToHostOrder(*(long*)ptr);
        }

        public long GetLastTombstoneEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones),
                throwIfDoesNotExist: false);

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table
                .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue)
                .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Reader.Read(1, out size);
            return Bits.SwapBytes(*(long*)ptr);
        }

        public long GetNumberOfTombstonesWithDocumentEtagLowerThan(DocumentsOperationContext context, string collection, long etag)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones),
                throwIfDoesNotExist: false);

            if (table == null)
                return 0;

            return table
                    .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[DeletedEtagsSlice], etag)
                    .Count();
        }



        public static Document TableValueToDocument(JsonOperationContext context, ref TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id
            };
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            byte offset;
            var ptr = tvr.Read(0, out size);
            result.LoweredKey = new LazyStringValue(null, ptr, size, context);

            ptr = tvr.Read(2, out size);
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);

            ptr = tvr.Read(1, out size);
            result.Etag = Bits.SwapBytes(*(long*)ptr);

            result.Data = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);

            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 4);

            result.LastModified = new DateTime(*(long*)tvr.Read(5, out size));

            result.Flags = (DocumentFlags)(*(int*)tvr.Read(6, out size));

            result.TransactionMarker = *(short*)tvr.Read(7, out size);

            return result;
        }

        private static DocumentConflict TableValueToConflictDocument(JsonOperationContext context, ref TableValueReader tvr)
        {
            var result = new DocumentConflict
            {
                StorageId = tvr.Id
            };
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            byte offset;
            var ptr = tvr.Read((int)ConflictsTable.LoweredKey, out size);
            result.LoweredKey = new LazyStringValue(null, ptr, size, context);

            ptr = tvr.Read((int)ConflictsTable.OriginalKey, out size);
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);
            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, (int)ConflictsTable.ChangeVector);
            var read = tvr.Read((int)ConflictsTable.Data, out size);
            if (size > 0)
            {
                //otherwise this is a tombstone conflict and should be treated as such
                result.Doc = new BlittableJsonReaderObject(read, size, context);
            }

            var etag = tvr.Read((int)ConflictsTable.Etag, out size);
            result.Etag = Bits.SwapBytes(*(long*)etag);

            result.Collection = new LazyStringValue(null, tvr.Read((int)ConflictsTable.Collection, out size), size,
                context);

            result.LastModified = new DateTime(*(long*)tvr.Read((int) ConflictsTable.LastModified, out size));

            return result;
        }

        private static ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(TableValueReader tvr, int index)
        {
            int size;
            var pChangeVector = (ChangeVectorEntry*)tvr.Read(index, out size);
            var changeVector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
            for (int i = 0; i < changeVector.Length; i++)
            {
                changeVector[i] = pChangeVector[i];
            }
            return changeVector;
        }

        private static DocumentTombstone TableValueToTombstone(JsonOperationContext context, ref TableValueReader tvr)
        {
            if (tvr == null) //precaution
                return null;

            var result = new DocumentTombstone
            {
                StorageId = tvr.Id
            };
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKeyAndCollection method
            var ptr = tvr.Read(0, out size);
            result.LoweredKey = new LazyStringValue(null, ptr, size, context);

            byte offset;
            ptr = tvr.Read(3, out size);
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);

            ptr = tvr.Read(1, out size);
            result.Etag = Bits.SwapBytes(*(long*)ptr);
            ptr = tvr.Read(2, out size);
            result.DeletedEtag = Bits.SwapBytes(*(long*)ptr);

            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 4);

            result.Collection = new LazyStringValue(null, tvr.Read(5, out size), size, context);

            result.TransactionMarker = *(short*)tvr.Read(6, out size);

            result.LastModified = new DateTime(*(long*)tvr.Read(7, out size));

            return result;
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, string key, long? expectedEtag)
        {
            Slice keySlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out keySlice))
            {
                return Delete(context, keySlice, key, expectedEtag);
            }
        }

        public struct DeleteOperationResult
        {
            public long Etag;
            public CollectionName Collection;
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context,
            Slice loweredKey,
            string key,
            long? expectedEtag,
            long? lastModifiedTicks = null,
            ChangeVectorEntry[] changeVector = null)
        {
            var result = GetDocumentOrTombstone(context, loweredKey);
            if (result.Item2 != null)
                return null; //NOP, already deleted

            var doc = result.Item1;
            if (doc == null)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException(
                        $"Document {loweredKey} does not exists, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                if (_hasConflicts != 0)
                    ThrowDocumentConflictIfNeeded(context, loweredKey);
                return null;
            }

            if (expectedEtag != null && doc.Etag != expectedEtag)
            {
                throw new ConcurrencyException(
                    $"Document {loweredKey} has etag {doc.Etag}, but Delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                {
                    ActualETag = doc.Etag,
                    ExpectedETag = (long)expectedEtag
                };
            }

            EnsureLastEtagIsPersisted(context, doc.Etag);

            var collectionName = ExtractCollectionName(context, loweredKey, doc.Data);
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

            int size;
            var ptr = table.DirectRead(doc.StorageId, out size);
            var tvr = new TableValueReader(ptr, size);

            int lowerSize;
            var lowerKey = tvr.Read(0, out lowerSize);

            int keySize;
            var keyPtr = tvr.Read(2, out keySize);

            var etag = CreateTombstone(context,
                lowerKey,
                lowerSize,
                keyPtr,
                keySize,
                doc.Etag,
                collectionName,
                doc.ChangeVector,
                lastModifiedTicks,
                changeVector);

            if (collectionName.IsSystem == false)
            {
                _documentDatabase.BundleLoader.VersioningStorage?.Delete(context, collectionName, loweredKey);
            }
            table.Delete(doc.StorageId);

            context.Transaction.AddAfterCommitNotification(new DocumentChange
            {
                Type = DocumentChangeTypes.Delete,
                Etag = expectedEtag,
                Key = key ?? loweredKey.ToString(),
                CollectionName = collectionName.Name,
                IsSystemDocument = collectionName.IsSystem,
            });

            return new DeleteOperationResult
            {
                Etag = etag,
                Collection = collectionName
            };
        }


        private static void ThrowDocumentConflictIfNeeded(DocumentsOperationContext context, Slice loweredKey)
        {
            var conflicts = GetConflictsFor(context, loweredKey);
            if (conflicts.Count > 0)
            {
                var changeVectors = new List<ChangeVectorEntry[]>();
                foreach (var conflict in conflicts)
                    changeVectors.Add(conflict.ChangeVector);

                throw new DocumentConflictException(loweredKey.ToString(), changeVectors);
            }
        }

        public long GenerateNextEtag()
        {
            return ++_lastEtag;
        }

        public void EnsureLastEtagIsPersisted(DocumentsOperationContext context, long docEtag)
        {
            if (docEtag != _lastEtag)
                return;
            var etagTree = context.Transaction.InnerTransaction.ReadTree("Etags");
            var etag = _lastEtag;
            Slice etagSlice;
            using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
                etagTree.Add(LastEtagSlice, etagSlice);
        }

        public void AddTombstoneOnReplicationIfRelevant(
            DocumentsOperationContext context,
            string key,
            long lastModifiedTicks,
            ChangeVectorEntry[] changeVector,
            string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: true);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);
            Slice loweredKey;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKey))
            {
                if (_hasConflicts != 0)
                    ThrowDocumentConflictIfNeeded(context, loweredKey);

                var result = GetDocumentOrTombstone(context, loweredKey);
                if (result.Item2 != null) //already have a tombstone -> need to update the change vector
                {
                    CreateTombstone(context,
                        lowerKey,
                        lowerSize,
                        keyPtr,
                        keySize,
                        result.Item2.Etag,
                        collectionName,
                        result.Item2.ChangeVector,
                        lastModifiedTicks,
                        changeVector);
                }
                else
                {
                    var doc = result.Item1;
                    var newEtag = GenerateNextEtag();

                    if (changeVector == null)
                    {
                        changeVector = GetMergedConflictChangeVectorsAndDeleteConflicts(
                            context,
                            loweredKey,
                            newEtag,
                            doc?.ChangeVector);
                    }

                    CreateTombstone(context,
                        lowerKey,
                        lowerSize,
                        keyPtr,
                        keySize,
                        doc?.Etag ?? -1,
                        //if doc == null, this means the tombstone does not have a document etag to point to
                        collectionName,
                        doc?.ChangeVector,
                        lastModifiedTicks,
                        changeVector);

                    // not sure if this needs to be done. 
                    // see http://issues.hibernatingrhinos.com/issue/RavenDB-5226
                    //if (isSystemDocument == false)
                    //{
                    // _documentDatabase.BundleLoader.VersioningStorage?.Delete(context, originalCollectionName, loweredKey);
                    //}

                    //it is possible that tombstones will be replicated and no document will be there 
                    //on the destination
                    if (doc != null)
                    {
                        var docsTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                            collectionName.GetTableName(CollectionTableType.Documents));
                        docsTable.Delete(doc.StorageId);
                    }

                    context.Transaction.AddAfterCommitNotification(new DocumentChange
                    {
                        Type = DocumentChangeTypes.DeleteOnTombstoneReplication,
                        Etag = newEtag,
                        CollectionName = collectionName.Name,
                        IsSystemDocument = false, //tombstone is not a system document...
                    });
                }
            }
        }
        
        private long CreateTombstone(
            DocumentsOperationContext context,
            byte* lowerKey, int lowerSize,
            byte* keyPtr, int keySize,
            long etag,
            CollectionName collectionName,
            ChangeVectorEntry[] docChangeVector,
            long? lastModifiedTicks,
            ChangeVectorEntry[] changeVector)
        {
            var newEtag = GenerateNextEtag();
            var newEtagBigEndian = Bits.SwapBytes(newEtag);
            var documentEtagBigEndian = Bits.SwapBytes(etag);

            if (changeVector == null)
            {
                Slice loweredKey;
                using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKey))
                {
                    changeVector = GetMergedConflictChangeVectorsAndDeleteConflicts(
                        context,
                        loweredKey,
                        newEtag,
                        docChangeVector);
                }
            }

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                Slice collectionSlice;
                using (Slice.From(context.Allocator, collectionName.Name, out collectionSlice))
                {
                    var transactionMarker = context.GetTransactionMarker();
                    var modifiedTicks = lastModifiedTicks ?? _documentDatabase.Time.GetUtcNow().Ticks;
                    var tbv = new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) &newEtagBigEndian, sizeof(long)},
                        {(byte*) &documentEtagBigEndian, sizeof(long)},
                        {keyPtr, keySize},
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                        collectionSlice,
                        transactionMarker,
                        modifiedTicks
                    };

                    var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                        collectionName.GetTableName(CollectionTableType.Tombstones));

                    table.Insert(tbv);
                }
            }
            return newEtag;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, string key)
        {
            if (_hasConflicts == 0)
                return;


            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            Slice keySlice;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out keySlice))
            {
                DeleteConflictsFor(context, keySlice);
            }
        }


        public IReadOnlyList<ChangeVectorEntry[]> DeleteConflictsFor(DocumentsOperationContext context, Slice loweredKey)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            var list = new List<ChangeVectorEntry[]>();
            bool deleted = true;
            while (deleted)
            {
                deleted = false;
                // deleting a value might cause other ids to change, so we can't just pass the list
                // of ids to be deleted, because they wouldn't remain stable during the deletions
                foreach (var result in conflictsTable.SeekForwardFrom(
                    ConflictsSchema.Indexes[KeyAndChangeVectorSlice],
                    loweredKey, true))
                {
                    foreach (var tvr in result.Results)
                    {
                        deleted = true;

                        int size;
                        var etag = *(long*)tvr.Reader.Read((int)ConflictsTable.Etag, out size);
                        var cve = tvr.Reader.Read((int)ConflictsTable.ChangeVector, out size);
                        var vector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
                        fixed (ChangeVectorEntry* pVector = vector)
                        {
                            Memory.Copy((byte*)pVector, cve, size);
                        }
                        list.Add(vector);
                        conflictsTable.Delete(tvr.Reader.Id);
                        EnsureLastEtagIsPersisted(context, etag);
                        break;
                    }
                }
            }

            // once this value has been set, we can't set it to false
            // an older transaction may be running and seeing it is false it
            // will not detect a conflict. It is an optimization only that
            // we have to do, so we'll handle it.

            //Only register the event if we actually deleted any conflicts
            var listCount = list.Count;
            if (listCount > 0)
            {
                var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
                tx.AfterCommitWhenNewReadTransactionsPrevented += () =>
                {
                    Interlocked.Add(ref _hasConflicts, -listCount);
                };
            }
            return list;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, ChangeVectorEntry[] changeVector)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                Slice changeVectorSlice;
                using (Slice.External(context.Allocator, (byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVector.Length, out changeVectorSlice))
                {
                    if (conflictsTable.DeleteByKey(changeVectorSlice))
                    {
                        var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
                        tx.AfterCommitWhenNewReadTransactionsPrevented += () =>
                        {
                            Interlocked.Decrement(ref _hasConflicts);
                        };
                    }
                }
            }
        }



        public DocumentConflict GetConflictForChangeVector(
            DocumentsOperationContext context,
            string key,
            ChangeVectorEntry[] changeVector)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            Slice loweredKeySlice;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKeySlice))
            {
                foreach (var result in conflictsTable.SeekForwardFrom(
                    ConflictsSchema.Indexes[KeyAndChangeVectorSlice],
                    loweredKeySlice, true))
                {
                    foreach (var r in result.Results)
                    {
                        int conflictKeySize;
                        var conflictKey = r.Reader.Read((int)ConflictsTable.LoweredKey, out conflictKeySize);

                        if (conflictKeySize != lowerSize)
                            break;

                        var compare = Memory.Compare(lowerKey, conflictKey, lowerSize);
                        if (compare != 0)
                            break;

                        var currentChangeVector = GetChangeVectorEntriesFromTableValueReader(r.Reader, (int)ConflictsTable.ChangeVector);
                        if (currentChangeVector.SequenceEqual(changeVector))
                        {
                            int size;
                            var dataPtr = r.Reader.Read((int)ConflictsTable.Data, out size);
                            return new DocumentConflict
                            {
                                ChangeVector = currentChangeVector,
                                Key = new LazyStringValue(key, r.Reader.Read((int)ConflictsTable.OriginalKey, out size), size, context),
                                StorageId = r.Reader.Id,
                                //size == 0 --> this is a tombstone conflict
                                Doc = (size == 0) ? null : new BlittableJsonReaderObject(dataPtr, size, context)
                            };
                        }
                    }
                }
            }
            return null;
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, string key)
        {
            if (_hasConflicts == 0)
                return ImmutableAppendOnlyList<DocumentConflict>.Empty;

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);
            Slice loweredKey;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKey))
            {
                return GetConflictsFor(context, loweredKey);
            }
        }

        private static IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, Slice loweredKey)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");
            var items = new List<DocumentConflict>();
            foreach (var result in conflictsTable.SeekForwardFrom(
                ConflictsSchema.Indexes[KeyAndChangeVectorSlice],
                loweredKey, true))
            {
                foreach (var tvr in result.Results)
                {
                    int conflictKeySize;
                    var conflictKey = tvr.Reader.Read((int)ConflictsTable.LoweredKey, out conflictKeySize);

                    if (conflictKeySize != loweredKey.Size)
                        break;

                    var compare = Memory.Compare(loweredKey.Content.Ptr, conflictKey, loweredKey.Size);
                    if (compare != 0)
                        break;

                    items.Add(TableValueToConflictDocument(context, ref tvr.Reader));
                }
            }

            return items;
        }

        public bool TryResolveIdenticalDocument(DocumentsOperationContext context, string key,
            BlittableJsonReaderObject incomingDoc,
            long lastModifiedTicks,
            ChangeVectorEntry[] incomingChangeVector)
        {
            var existing = GetDocumentOrTombstone(context, key, throwOnConflict: false);
            var existingDoc = existing.Item1;
            var existingTombstone = existing.Item2;

            if (existingDoc != null && existingDoc.IsMetadataEqualTo(incomingDoc) &&
                    existingDoc.IsEqualTo(incomingDoc))
            {
                // no real conflict here, both documents have identical content
                existingDoc.ChangeVector = ReplicationUtils.MergeVectors(incomingChangeVector, existingDoc.ChangeVector);
                Put(context, existingDoc.Key, null, existingDoc.Data, lastModifiedTicks, existingDoc.ChangeVector);
                return true;
            }

            if (existingTombstone != null && incomingDoc == null)
            {
                // Conflict between two tombstones resolves to the local tombstone
                existingTombstone.ChangeVector = ReplicationUtils.MergeVectors(incomingChangeVector, existingTombstone.ChangeVector);
                AddTombstoneOnReplicationIfRelevant(context, existingTombstone.Key,
                    lastModifiedTicks,
                    existingTombstone.ChangeVector,
                    existingTombstone.Collection);
                return true;
            }

            return false;
        }

        public void PutResolvedDocumentBackToStorage(
            DocumentsOperationContext ctx,
            DocumentConflict conflict,
            bool hasLocalTombstone)
        {
            if (conflict.Doc == null)
            {
                if (hasLocalTombstone) // local document is a tombstone
                {
                    // update change vector
                    AddTombstoneOnReplicationIfRelevant(
                    ctx, conflict.LoweredKey, _documentDatabase.Time.GetUtcNow().Ticks, conflict.ChangeVector, conflict.Collection);
                    return;
                }
                // the resolved document is a tombstone
                DeleteConflictsFor(ctx, conflict.LoweredKey);
                AddTombstoneOnReplicationIfRelevant(
                    ctx, conflict.LoweredKey, _documentDatabase.Time.GetUtcNow().Ticks, conflict.ChangeVector, conflict.Collection);
                return;
            }

            // because we are resolving to a conflict, and putting a document will
            // delete all the conflicts, we have to create a copy of the document
            // in order to avoid the data we are saving from being removed while
            // we are saving it
            using (var clone = conflict.Doc.Clone(ctx))
            {
                ReplicationUtils.EnsureCollectionTag(clone, conflict.Collection);
                Put(ctx, conflict.LoweredKey, null, clone, null, conflict.ChangeVector);
            }
        }

        public bool TryResolveConflictByScriptInternal(
            DocumentsOperationContext context,
            ScriptResolver scriptResolver,
            IReadOnlyList<DocumentConflict> conflicts,
            LazyStringValue collection,
            bool hasLocalTombstone)
        {
            var patch = new PatchConflict(_documentDatabase, conflicts);
            var updatedConflict = conflicts[0];
            var patchRequest = new PatchRequest
            {
                Script = scriptResolver.Script
            };
            BlittableJsonReaderObject resolved;
            if (patch.TryResolveConflict(context, patchRequest, out resolved) == false)
            {
                return false;
            }

            updatedConflict.Doc = resolved;
            updatedConflict.Collection = collection;
            updatedConflict.ChangeVector = ReplicationUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
            PutResolvedDocumentBackToStorage(context, updatedConflict, hasLocalTombstone);
            return true;
        }

        public bool TryResolveUsingDefaultResolverInternal(
            DocumentsOperationContext context,
            DatabaseResolver resolver,
            IReadOnlyList<DocumentConflict> conflicts,
            bool hasTombstoneInStorage)
        {
            if (resolver?.ResolvingDatabaseId == null)
            {
                return false;
            }
            
            DocumentConflict resolved = null;
            long maxEtag = -1;
            foreach (var documentConflict in conflicts)
            {
                foreach (var changeVectorEntry in documentConflict.ChangeVector)
                {
                    if (changeVectorEntry.DbId.Equals(new Guid(resolver.ResolvingDatabaseId)))
                    {
                        if (changeVectorEntry.Etag == maxEtag)
                        {
                            // we have two documents with same etag of the leader
                            return false;
                        }

                        if (changeVectorEntry.Etag < maxEtag)
                            continue;

                        maxEtag = changeVectorEntry.Etag;
                        resolved = documentConflict;
                        break;
                    }
                }
            }

            if (resolved == null)
                return false;
           
            resolved.ChangeVector = ReplicationUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
            PutResolvedDocumentBackToStorage(context, resolved, hasTombstoneInStorage);
            return true;
        }

        public void ResolveToLatest(
            DocumentsOperationContext context,
            IReadOnlyList<DocumentConflict> conflicts,
            bool hasLocalTombstone)
        {
            var latestDoc = conflicts[0];
            var latestTime = latestDoc.LastModified.Ticks;

            foreach (var documentConflict in conflicts)
            {
                if (documentConflict.LastModified.Ticks > latestTime)
                {
                    latestDoc = documentConflict;
                    latestTime = documentConflict.LastModified.Ticks;
                }
            }

            latestDoc.ChangeVector = ReplicationUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
            PutResolvedDocumentBackToStorage(context, latestDoc, hasLocalTombstone);
        }

        public void AddConflict(
            DocumentsOperationContext context,
            IncomingReplicationHandler.ReplicationDocumentsPositions docPositions,
            BlittableJsonReaderObject incomingDoc,
            ChangeVectorEntry[] incomingChangeVector,
            string incomingTombstoneCollection)
        {
            var key = docPositions.Id;
            if (_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {key} (Incoming change vector {incomingChangeVector.Format()})");
            var tx = context.Transaction.InnerTransaction;
            var conflictsTable = tx.OpenTable(ConflictsSchema, "Conflicts");

            CollectionName collectionName;

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);
            // ReSharper disable once ArgumentsStyleLiteral
            var existing = GetDocumentOrTombstone(context, key, throwOnConflict: false);
            if (existing.Item1 != null)
            {
                var existingDoc = existing.Item1;

                fixed (ChangeVectorEntry* pChangeVector = existingDoc.ChangeVector)
                {
                    var lazyCollectionName = CollectionName.GetLazyCollectionNameFrom(context, existingDoc.Data);
                    conflictsTable.Set(new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, existingDoc.ChangeVector.Length*sizeof(ChangeVectorEntry)},
                        {keyPtr, keySize},
                        {existingDoc.Data.BasePointer, existingDoc.Data.Size},
                        Bits.SwapBytes(GenerateNextEtag()),
                        {lazyCollectionName.Buffer, lazyCollectionName.Size},
                        existingDoc.LastModified.Ticks
                    });
                    Interlocked.Increment(ref _hasConflicts);
                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    EnsureLastEtagIsPersisted(context, existingDoc.Etag);
                    collectionName = ExtractCollectionName(context, existingDoc.Key, existingDoc.Data);

                    //make sure that the relevant collection tree exists
                    var table = tx.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                    table.Delete(existingDoc.StorageId);
                }
            }
            else if (existing.Item2 != null)
            {
                var existingTombstone = existing.Item2;

                fixed (ChangeVectorEntry* pChangeVector = existingTombstone.ChangeVector)
                {
                    conflictsTable.Set(new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, existingTombstone.ChangeVector.Length*sizeof(ChangeVectorEntry)},
                        {keyPtr, keySize},
                        {null, 0},
                        Bits.SwapBytes(GenerateNextEtag()),
                        {existingTombstone.Collection.Buffer, existingTombstone.Collection.Size},
                        existingTombstone.LastModified.Ticks
                    });
                    Interlocked.Increment(ref _hasConflicts);
                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    EnsureLastEtagIsPersisted(context, existingTombstone.Etag);

                    collectionName = GetCollection(existingTombstone.Collection, throwIfDoesNotExist: true);

                    var table = tx.OpenTable(TombstonesSchema,
                        collectionName.GetTableName(CollectionTableType.Tombstones));
                    table.Delete(existingTombstone.StorageId);
                }
            }
            else // has existing conflicts
            {
                collectionName = ExtractCollectionName(context, key, incomingDoc);

                Slice loweredKeySlice;
                using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKeySlice))
                {
                    var conflicts = GetConflictsFor(context, loweredKeySlice);
                    foreach (var conflict in conflicts)
                    {
                        var conflictStatus = IncomingReplicationHandler.GetConflictStatus(incomingChangeVector, conflict.ChangeVector);
                        switch (conflictStatus)
                        {
                            case IncomingReplicationHandler.ConflictStatus.Update:
                                DeleteConflictsFor(context, conflict.ChangeVector); // delete this, it has been subsumed
                                break;
                            case IncomingReplicationHandler.ConflictStatus.Conflict:
                                break; // we'll add this conflict if no one else also includes it
                            case IncomingReplicationHandler.ConflictStatus.AlreadyMerged:
                                return; // we already have a conflict that includes this version
                            // ReSharper disable once RedundantCaseLabel
                            case IncomingReplicationHandler.ConflictStatus.ShouldResolveConflict:
                            default:
                                throw new ArgumentOutOfRangeException("Invalid conflict status " + conflictStatus);
                        }
                    }
                }
            }
            fixed (ChangeVectorEntry* pChangeVector = incomingChangeVector)
            {
                byte* doc = null;
                int docSize = 0;
                LazyStringValue lazyCollectioName;
                if (incomingDoc != null) // can be null if it is a tombstone
                {
                    doc = incomingDoc.BasePointer;
                    docSize = incomingDoc.Size;
                    lazyCollectioName = CollectionName.GetLazyCollectionNameFrom(context, incomingDoc);
                }
                else
                {
                    lazyCollectioName = context.GetLazyString(incomingTombstoneCollection);
                }

                using (lazyCollectioName)
                {
                    var tvb = new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*incomingChangeVector.Length},
                        {keyPtr, keySize},
                        {doc, docSize},
                        Bits.SwapBytes(GenerateNextEtag()),
                        {lazyCollectioName.Buffer, lazyCollectioName.Size},
                        docPositions.LastModifiedTicks
                    };

                    Interlocked.Increment(ref _hasConflicts);
                    conflictsTable.Set(tvb);
                }

            }

            context.Transaction.AddAfterCommitNotification(new DocumentChange
            {
                Etag = _lastEtag,
                CollectionName = collectionName.Name,
                Key = key,
                Type = DocumentChangeTypes.Conflict,
                IsSystemDocument = false,
            });
        }

        public struct PutOperationResults
        {
            public string Key;
            public long Etag;
            public CollectionName Collection;
        }

        public void DeleteWithoutCreatingTombstone(DocumentsOperationContext context, string collection, long storageId, bool isTombstone)
        {
            // we delete the data directly, without generating a tombstone, because we have a 
            // conflict instead
            var tx = context.Transaction.InnerTransaction;

            var collectionObject = new CollectionName(collection);
            var collectionName = isTombstone ?
                collectionObject.GetTableName(CollectionTableType.Tombstones) :
                collectionObject.GetTableName(CollectionTableType.Documents);

            //make sure that the relevant collection tree exists
            Table table = isTombstone ?
                tx.OpenTable(TombstonesSchema, collectionName) :
                tx.OpenTable(DocsSchema, collectionName);

            table.Delete(storageId);
        }

        public PutOperationResults Put(DocumentsOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document,
            long? lastModifiedTicks = null,
            ChangeVectorEntry[] changeVector = null)
        {
            if (context.Transaction == null)
            {
                ThrowPutRequiresTransaction();
                return default(PutOperationResults);// never reached
            }

            AssertNoModifications(document, key);

            var collectionName = ExtractCollectionName(context, key, document);
            var newEtag = GenerateNextEtag();
            var newEtagBigEndian = Bits.SwapBytes(newEtag);
            int flags = 0;
            if (collectionName.IsSystem == false)
            {
                bool hasVersion =
                    _documentDatabase.BundleLoader.VersioningStorage?.PutFromDocument(context, collectionName,
                    key, document) ?? false;
                if (hasVersion)
                {
                    flags = (int)DocumentFlags.Versioned;
                }
            }

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

            bool knownNewKey = false;

            if (string.IsNullOrWhiteSpace(key))
            {
                key = Guid.NewGuid().ToString();
                knownNewKey = true;
            }

            switch (key[key.Length - 1])
            {
                case '/':
                    int tries;
                    key = GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, table, context, out tries);
                    knownNewKey = true;
                    break;
                case '|':
                    key = AppendNumericValueToKey(key, newEtag);
                    knownNewKey = true;
                    break;
            }


            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            if (_hasConflicts != 0)
                changeVector = MergeConflictChangeVectorIfNeededAndDeleteConflicts(changeVector, context, key, newEtag);

            // delete a tombstone if it exists, if it known that it is a new key, no need, so we can skip it
            if (knownNewKey == false)
            {
                DeleteTombstoneIfNeeded(context, collectionName, lowerKey, lowerSize);
            }


            var modifiedTicks = lastModifiedTicks ?? _documentDatabase.Time.GetUtcNow().Ticks;

            Slice keySlice;
            using (Slice.External(context.Allocator, lowerKey, (ushort)lowerSize, out keySlice))
            {
                TableValueReader oldValue = null;
                if (knownNewKey == false)
                {
                    table.ReadByKey(keySlice, out oldValue);
                }

                if (changeVector == null)
                {
                    changeVector = SetDocumentChangeVectorForLocalChange(context,
                        keySlice,
                        oldValue, newEtag);
                }

                fixed (ChangeVectorEntry* pChangeVector = changeVector)
                {
                    var transactionMarker = context.GetTransactionMarker();
                    var tbv = new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        newEtagBigEndian,
                        {keyPtr, keySize},
                        {document.BasePointer, document.Size},
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                        modifiedTicks,
                        flags,
                        transactionMarker
                    };

                    if (oldValue == null)
                    {
                        if (expectedEtag != null && expectedEtag != 0)
                        {
                            ThrowConcurrentExceptionOnMissingDoc(key, expectedEtag);
                        }
                        table.Insert(tbv);
                    }
                    else
                    {
                        int size;
                        var pOldEtag = oldValue.Read(1, out size);
                        var oldEtag = Bits.SwapBytes(*(long*)pOldEtag);
                        //TODO
                        if (expectedEtag != null && oldEtag != expectedEtag)
                            ThrowConcurrentException(key, expectedEtag, oldEtag);

                        int oldSize;
                        var oldDoc = new BlittableJsonReaderObject(oldValue.Read(3, out oldSize), oldSize, context);
                        var oldCollectionName = ExtractCollectionName(context, key, oldDoc);
                        if (oldCollectionName != collectionName)
                            ThrowInvalidCollectionNameChange(key, oldCollectionName, collectionName);

                        table.Update(oldValue.Id, tbv);
                    }
                }

                if (collectionName.IsSystem == false)
                {

                    _documentDatabase.BundleLoader.ExpiredDocumentsCleaner?.Put(context,
                        keySlice, document);
                }

                _documentDatabase.Metrics.DocPutsPerSecond.MarkSingleThreaded(1);
                _documentDatabase.Metrics.BytesPutsPerSecond.MarkSingleThreaded(document.Size);
            }

            context.Transaction.AddAfterCommitNotification(new DocumentChange
            {
                Etag = newEtag,
                CollectionName = collectionName.Name,
                Key = key,
                Type = DocumentChangeTypes.Put,
                IsSystemDocument = collectionName.IsSystem,
            });


            return new PutOperationResults
            {
                Etag = newEtag,
                Key = key,
                Collection = collectionName
            };
        }

        private static void ThrowConcurrentExceptionOnMissingDoc(string key, long? expectedEtag)
        {
            throw new ConcurrencyException(
                $"Document {key} does not exists, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedETag = (long)expectedEtag
            };
        }

        private static void ThrowInvalidCollectionNameChange(string key, CollectionName oldCollectionName,
            CollectionName collectionName)
        {
            throw new InvalidOperationException(
                $"Changing '{key}' from '{oldCollectionName.Name}' to '{collectionName.Name}' via update is not supported.{System.Environment.NewLine}" +
                $"Delete the document and recreate the document {key}.");
        }

        private static void ThrowConcurrentException(string key, long? expectedEtag, long oldEtag)
        {
            throw new ConcurrencyException(
                $"Document {key} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ActualETag = oldEtag,
                ExpectedETag = expectedEtag ?? -1
            };
        }

        private ChangeVectorEntry[] MergeConflictChangeVectorIfNeededAndDeleteConflicts(ChangeVectorEntry[] documentChangeVector, DocumentsOperationContext context, string key, long newEtag)
        {
            ChangeVectorEntry[] mergedChangeVectorEntries = null;
            bool firstTime = true;
            foreach (var conflict in GetConflictsFor(context, key))
            {
                if (firstTime)
                {
                    mergedChangeVectorEntries = conflict.ChangeVector;
                    firstTime = false;
                    continue;
                }
                mergedChangeVectorEntries = ReplicationUtils.MergeVectors(mergedChangeVectorEntries, conflict.ChangeVector);
            }
            //We had conflicts need to delete them
            if (mergedChangeVectorEntries != null)
            {

                DeleteConflictsFor(context, key);
                if (documentChangeVector != null)
                    mergedChangeVectorEntries =  ReplicationUtils.MergeVectors(mergedChangeVectorEntries, documentChangeVector);

                mergedChangeVectorEntries = ReplicationUtils.MergeVectors(mergedChangeVectorEntries, new[]
                {
                    new ChangeVectorEntry
                    {
                        DbId = _documentDatabase.DbId,
                        Etag = newEtag
                    }
                });

                return mergedChangeVectorEntries;
            }
            return documentChangeVector; // this covers the null && null case too
        }

        private static void ThrowPutRequiresTransaction()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentException("Context must be set with a valid transaction before calling Put", "context");
        }

        private static void DeleteTombstoneIfNeeded(DocumentsOperationContext context, CollectionName collectionName, byte* lowerKey, int lowerSize)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            Slice key;
            using (Slice.From(context.Allocator, lowerKey, lowerSize, out key))
            {
                tombstoneTable.DeleteByKey(key);
            }
        }

        private ChangeVectorEntry[] SetDocumentChangeVectorForLocalChange(
            DocumentsOperationContext context, Slice loweredKey,
            TableValueReader oldValue, long newEtag)
        {
            if (oldValue != null)
            {
                var changeVector = GetChangeVectorEntriesFromTableValueReader(oldValue, 4);
                return ReplicationUtils.UpdateChangeVectorWithNewEtag(Environment.DbId, newEtag, changeVector);
            }

            return GetMergedConflictChangeVectorsAndDeleteConflicts(context, loweredKey, newEtag);
        }

        private ChangeVectorEntry[] GetMergedConflictChangeVectorsAndDeleteConflicts(
            DocumentsOperationContext context,
            Slice loweredKey,
            long newEtag,
            ChangeVectorEntry[] existing = null)
        {
            if (_hasConflicts == 0)
                return MergeVectorsWithoutConflicts(newEtag, existing);

            var conflictChangeVectors = DeleteConflictsFor(context, loweredKey);
            if (conflictChangeVectors.Count == 0)
                return MergeVectorsWithoutConflicts(newEtag, existing);

            // need to merge the conflict change vectors
            var maxEtags = new Dictionary<Guid, long>
            {
                [Environment.DbId] = newEtag
            };

            foreach (var conflictChangeVector in conflictChangeVectors)
                foreach (var entry in conflictChangeVector)
                {
                    long etag;
                    if (maxEtags.TryGetValue(entry.DbId, out etag) == false ||
                        etag < entry.Etag)
                    {
                        maxEtags[entry.DbId] = entry.Etag;
                    }
                }

            var changeVector = new ChangeVectorEntry[maxEtags.Count];

            var index = 0;
            foreach (var maxEtag in maxEtags)
            {
                changeVector[index].DbId = maxEtag.Key;
                changeVector[index].Etag = maxEtag.Value;
                index++;
            }
            return changeVector;
        }

        private ChangeVectorEntry[] MergeVectorsWithoutConflicts(long newEtag, ChangeVectorEntry[] existing)
        {
            if (existing != null)
                return ReplicationUtils.UpdateChangeVectorWithNewEtag(Environment.DbId, newEtag, existing);

            return new[]
            {
                new ChangeVectorEntry
                {
                    Etag = newEtag,
                    DbId = Environment.DbId
                }
            };
        }

        public IEnumerable<KeyValuePair<string, long>> GetIdentities(DocumentsOperationContext context)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree("Identities");
            using (var it = identities.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    var name = it.CurrentKey.ToString();
                    var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                    yield return new KeyValuePair<string, long>(name, value);
                } while (it.MoveNext());
            }
        }

        public string GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key, Table table, DocumentsOperationContext context, out int tries)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree("Identities");
            var nextIdentityValue = identities.Increment(key, 1);
            var finalKey = AppendIdentityValueToKey(key, nextIdentityValue);
            Slice finalKeySlice;
            tries = 1;

            using (DocumentKeyWorker.GetSliceFromKey(context, finalKey, out finalKeySlice))
            {
                TableValueReader reader;
                if (table.ReadByKey(finalKeySlice,out reader) == false)
                {
                    return finalKey;
                }
            }

            /* We get here if the user inserted a document with a specified id.
            e.g. your identity is 100
            but you forced a put with 101
            so you are trying to insert next document and it would overwrite the one with 101 */

            var lastKnownBusy = nextIdentityValue;
            var maybeFree = nextIdentityValue * 2;
            var lastKnownFree = long.MaxValue;
            while (true)
            {
                tries++;
                finalKey = AppendIdentityValueToKey(key, maybeFree);
                using (DocumentKeyWorker.GetSliceFromKey(context, finalKey, out finalKeySlice))
                {
                    TableValueReader reader;
                    if (table.ReadByKey(finalKeySlice,out reader) == false)
                    {
                        if (lastKnownBusy + 1 == maybeFree)
                        {
                            nextIdentityValue = identities.Increment(key, lastKnownBusy);
                            return key + nextIdentityValue;
                        }
                        lastKnownFree = maybeFree;
                        maybeFree = Math.Max(maybeFree - (maybeFree - lastKnownBusy) / 2, lastKnownBusy + 1);
                    }
                    else
                    {
                        lastKnownBusy = maybeFree;
                        maybeFree = Math.Min(lastKnownFree, maybeFree * 2);
                    }
                }
            }
        }

        private string AppendIdentityValueToKey(string key, long val)
        {
            _keyBuilder.Length = 0;
            _keyBuilder.Append(key);
            _keyBuilder.Append(val);
            return _keyBuilder.ToString();
        }


        private string AppendNumericValueToKey(string key, long val)
        {
            _keyBuilder.Length = 0;
            _keyBuilder.Append(key);
            _keyBuilder[_keyBuilder.Length - 1] = '/';
            _keyBuilder.AppendFormat(CultureInfo.InvariantCulture, "D19", val);
            return _keyBuilder.ToString();
        }

        public long IdentityFor(DocumentsOperationContext ctx, string key)
        {
            var identities = ctx.Transaction.InnerTransaction.ReadTree("Identities");
            return identities.Increment(key, 1);
        }

        public long GetNumberOfDocumentsToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
        {
            return GetNumberOfItemsToProcess(context, collection, afterEtag, tombstones: false, totalCount: out totalCount);
        }

        public long GetNumberOfTombstonesToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
        {
            return GetNumberOfItemsToProcess(context, collection, afterEtag, tombstones: true, totalCount: out totalCount);
        }

        private long GetNumberOfItemsToProcess(DocumentsOperationContext context, string collection, long afterEtag, bool tombstones, out long totalCount)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            Table table;
            TableSchema.FixedSizeSchemaIndexDef indexDef;
            if (tombstones)
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                    collectionName.GetTableName(CollectionTableType.Tombstones),
                    throwIfDoesNotExist: false);

                indexDef = TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }
            else
            {
                table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                    collectionName.GetTableName(CollectionTableType.Documents),
                    throwIfDoesNotExist: false);
                indexDef = DocsSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }
            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            return table.GetNumberEntriesFor(indexDef, afterEtag, out totalCount);
        }

        public long GetNumberOfDocuments()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
                return GetNumberOfDocuments(context);
        }

        public long GetNumberOfDocuments(DocumentsOperationContext context)
        {
            var fstIndex = DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public class CollectionStats
        {
            public string Name;
            public long Count;
        }

        public IEnumerable<CollectionStats> GetCollections(DocumentsOperationContext context)
        {
            foreach (var kvp in _collectionsCache)
            {
                var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema, kvp.Value.GetTableName(CollectionTableType.Documents));

                yield return new CollectionStats
                {
                    Name = kvp.Key,
                    Count = collectionTable.NumberOfEntries
                };
            }
        }

        public CollectionStats GetCollection(string collection, DocumentsOperationContext context)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                return new CollectionStats
                {
                    Name = collection,
                    Count = 0
                };
            }

            var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents),
                throwIfDoesNotExist: false);

            if (collectionTable == null)
            {
                return new CollectionStats
                {
                    Name = collection,
                    Count = 0
                };
            }

            return new CollectionStats
            {
                Name = collectionName.Name,
                Count = collectionTable.NumberOfEntries
            };
        }

        public void DeleteTombstonesBefore(string collection, long etag, Transaction transaction)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return;

            var table = transaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones),
                throwIfDoesNotExist: false);
            if (table == null)
                return;

            var deleteCount = table.DeleteBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, long.MaxValue);
            if (_logger.IsInfoEnabled && deleteCount > 0)
                _logger.Info($"Deleted {deleteCount:#,#;;0} tombstones earlier than {etag} in {collection}");

        }

        public IEnumerable<string> GetTombstoneCollections(Transaction transaction)
        {
            using (var it = transaction.LowLevelTransaction.RootObjects.Iterate(false))
            {
                it.RequiredPrefix = TombstonesPrefix;

                if (it.Seek(TombstonesPrefix) == false)
                    yield break;

                do
                {
                    var tombstoneCollection = it.CurrentKey.ToString();
                    yield return tombstoneCollection.Substring(TombstonesPrefix.Size);
                }
                while (it.MoveNext());
            }
        }

        public void UpdateIdentities(DocumentsOperationContext context, Dictionary<string, long> identities)
        {
            var readTree = context.Transaction.InnerTransaction.ReadTree("Identities");
            foreach (var identity in identities)
            {
                readTree.AddMax(identity.Key, identity.Value);
            }
        }

        public long GetLastReplicateEtagFrom(DocumentsOperationContext context, string dbId)
        {
            var readTree = context.Transaction.InnerTransaction.ReadTree("LastReplicatedEtags");
            var readResult = readTree.Read(dbId);
            if (readResult == null)
                return 0;
            return readResult.Reader.ReadLittleEndianInt64();
        }

        public void SetLastReplicateEtagFrom(DocumentsOperationContext context, string dbId, long etag)
        {
            var etagsTree = context.Transaction.InnerTransaction.CreateTree("LastReplicatedEtags");
            Slice etagSlice;
            Slice keySlice;
            using (Slice.From(context.Allocator, dbId, out keySlice))
            using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
            {
                etagsTree.Add(keySlice, etagSlice);
            }
        }

        private CollectionName GetCollection(string collection, bool throwIfDoesNotExist)
        {
            CollectionName collectionName;
            if (_collectionsCache.TryGetValue(collection, out collectionName) == false && throwIfDoesNotExist)
                throw new InvalidOperationException($"There is no collection for '{collection}'.");

            return collectionName;
        }

        private CollectionName ExtractCollectionName(DocumentsOperationContext context, string key, BlittableJsonReaderObject document)
        {
            var originalCollectionName = CollectionName.GetCollectionName(key, document);

            return ExtractCollectionName(context, originalCollectionName);
        }

        private CollectionName ExtractCollectionName(DocumentsOperationContext context, Slice key, BlittableJsonReaderObject document)
        {
            var originalCollectionName = CollectionName.GetCollectionName(key, document);

            return ExtractCollectionName(context, originalCollectionName);
        }

        private CollectionName ExtractCollectionName(DocumentsOperationContext context, string collectionName)
        {
            CollectionName name;
            if (_collectionsCache.TryGetValue(collectionName, out name))
                return name;

            var collections = context.Transaction.InnerTransaction.OpenTable(CollectionsSchema, "Collections");

            name = new CollectionName(collectionName);

            Slice collectionSlice;
            using (Slice.From(context.Allocator, collectionName, out collectionSlice))
            {
                var tvr = new TableValueBuilder
                {
                    collectionSlice
                };
                collections.Set(tvr);

                DocsSchema.Create(context.Transaction.InnerTransaction, name.GetTableName(CollectionTableType.Documents), 16);
                TombstonesSchema.Create(context.Transaction.InnerTransaction,
                    name.GetTableName(CollectionTableType.Tombstones), 16);

                // Add to cache ONLY if the transaction was committed. 
                // this would prevent NREs next time a PUT is run,since if a transaction
                // is not commited, DocsSchema and TombstonesSchema will not be actually created..
                // has to happen after the commit, but while we are holding the write tx lock
                context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += _ =>
                {
                    var collectionNames = new Dictionary<string, CollectionName>(_collectionsCache,
                        StringComparer.OrdinalIgnoreCase);
                    collectionNames[name.Name] = name;
                    _collectionsCache = collectionNames;
                };
            }
            return name;
        }

        private Dictionary<string, CollectionName> ReadCollections(Transaction tx)
        {
            var result = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

            var collections = tx.OpenTable(CollectionsSchema, "Collections");
            
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                foreach (var tvr in collections.SeekByPrimaryKey(Slices.BeforeAllKeys))
                {
                    int size;
                    var ptr = tvr.Reader.Read(0, out size);
                    var collection = new LazyStringValue(null, ptr, size, context);

                    Slice tableNameSlice;
                    Slice.External(tx.Allocator, ptr, size, out tableNameSlice);// intentionally not disposing, will be disposed by the tx
                    var tableTree = tx.CreateTree(tableNameSlice, RootObjectType.Table);
                    NewPageAllocator.MaybePrefetchSections(tableTree, tx.LowLevelTransaction);


                    result.Add(collection, new CollectionName(collection));
                }
            }

            return result;
        }


        [Conditional("DEBUG")]
        internal static void AssertNoModifications(BlittableJsonReaderObject data, string key)
        {
            if (data == null)
                return;

            if (data.Modifications != null)
            {
                if (data.Modifications.Removals != null && data.Modifications.Removals.Count > 0)
                    throw new InvalidOperationException($"Modifications detected in '{key}'. JSON: {data}");

                if (data.Modifications.Properties.Count > 0)
                    throw new InvalidOperationException($"Modifications detected in '{key}'. JSON: {data}");
            }

            foreach (var propertyName in data.GetPropertyNames())
            {
                var inner = data[propertyName] as BlittableJsonReaderObject;
                if (inner == null)
                    continue;

                AssertNoModifications(inner, key);
            }
        }
    }
}