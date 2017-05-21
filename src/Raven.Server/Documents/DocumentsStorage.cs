using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions;
using Raven.Server.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Data;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private static readonly Slice DocsSlice;
        private static readonly Slice CollectionEtagsSlice;
        private static readonly Slice AllDocsEtagsSlice;
        private static readonly Slice TombstonesSlice;
        private static readonly Slice CollectionsSlice;
        private static readonly Slice LastReplicatedEtagsSlice;
        private static readonly Slice ChangeVectorSlice;
        private static readonly Slice EtagsSlice;
        private static readonly Slice LastEtagSlice;

        private static readonly Slice AllTombstonesEtagsSlice;
        private static readonly Slice TombstonesPrefix;
        private static readonly Slice DeletedEtagsSlice;

        public static readonly TableSchema DocsSchema = new TableSchema();
        public static readonly TableSchema TombstonesSchema = new TableSchema();
        private static readonly TableSchema CollectionsSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;

        private FastDictionary<string, CollectionName, OrdinalIgnoreCaseStringStructComparer> _collectionsCache;

        private enum TombstoneTable
        {
            LoweredKey = 0,
            Etag = 1,
            DeletedEtag = 2,
            TransactionMarker = 3,
            Type = 4,
            Collection = 5,
            Flags = 6,
            ChangeVector = 7,
            LastModified = 8,
        }

        public enum DocumentsTable
        {
            LoweredKey = 0,
            Etag = 1,
            Key = 2,
            Data = 3,
            ChangeVector = 4,
            LastModified = 5,
            Flags = 6,
            TransactionMarker = 7,
        }

        static DocumentsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "AllTombstonesEtags", ByteStringType.Immutable, out AllTombstonesEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Etags", ByteStringType.Immutable, out EtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastEtag", ByteStringType.Immutable, out LastEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Docs", ByteStringType.Immutable, out DocsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Tombstones", ByteStringType.Immutable, out TombstonesSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Collections", ByteStringType.Immutable, out CollectionsSlice);
            Slice.From(StorageEnvironment.LabelsContext, CollectionName.GetTablePrefix(CollectionTableType.Tombstones), ByteStringType.Immutable, out TombstonesPrefix);
            Slice.From(StorageEnvironment.LabelsContext, "DeletedEtags", ByteStringType.Immutable, out DeletedEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastReplicatedEtags", ByteStringType.Immutable, out LastReplicatedEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "ChangeVector", ByteStringType.Immutable, out ChangeVectorSlice);            
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

            // The documents schema is as follows
            // fields (lowered key, etag, lazy string key, document, change vector, last modified, optional flags)
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            DocsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)DocumentsTable.LoweredKey,
                Count = 1,
                IsGlobal = true,
                Name = DocsSlice
            });
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)DocumentsTable.Etag,
                IsGlobal = false,
                Name = CollectionEtagsSlice
            });
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)DocumentsTable.Etag,
                IsGlobal = true,
                Name = AllDocsEtagsSlice
            });

            TombstonesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.LoweredKey,
                Count = 1,
                IsGlobal = true,
                Name = TombstonesSlice
            });
            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.Etag,
                IsGlobal = false,
                Name = CollectionEtagsSlice
            });
            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.Etag,
                IsGlobal = true,
                Name = AllTombstonesEtagsSlice
            });
            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef()
            {
                StartIndex = (int)TombstoneTable.DeletedEtag,
                IsGlobal = false,
                Name = DeletedEtagsSlice
            });
        }

        private readonly Logger _logger;
        private readonly string _name;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        public DocumentsContextPool ContextPool;

        public DocumentsStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _name = _documentDatabase.Name;
            _logger = LoggingSource.Instance.GetLogger<DocumentsStorage>(documentDatabase.Name);
        }

        public StorageEnvironment Environment { get; private set; }

        public ConflictsStorage ConflictsStorage;
        public AttachmentsStorage AttachmentsStorage;
        public IdentitiesStorage Identities;
        public DocumentPutAction DocumentPut;

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentsStorage)}");

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
                ? StorageEnvironmentOptions.CreateMemoryOnly(
                    _documentDatabase.Configuration.Core.DataDirectory.FullPath,
                    null,
                    _documentDatabase.IoChanges,
                    _documentDatabase.CatastrophicFailureNotification)
                : StorageEnvironmentOptions.ForPath(
                    _documentDatabase.Configuration.Core.DataDirectory.FullPath,
                    _documentDatabase.Configuration.Storage.TempPath?.FullPath,
                    _documentDatabase.Configuration.Storage.JournalsStoragePath?.FullPath,
                    _documentDatabase.IoChanges,
                    _documentDatabase.CatastrophicFailureNotification
                    );

            options.OnNonDurableFileSystemError += _documentDatabase.HandleNonDurableFileSystemError;

            options.ForceUsing32BitsPager = _documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
            options.TimeToSyncAfterFlashInSeconds = _documentDatabase.Configuration.Storage.TimeToSyncAfterFlashInSeconds;
            options.NumOfCocurrentSyncsPerPhysDrive = _documentDatabase.Configuration.Storage.NumOfCocurrentSyncsPerPhysDrive;
            options.MasterKey = _documentDatabase.MasterKey;

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
            options.SchemaVersion = 5;
            try
            {
                Environment = new StorageEnvironment(options);
                ContextPool = new DocumentsContextPool(_documentDatabase);

                using (var tx = Environment.WriteTransaction())
                {
                    NewPageAllocator.MaybePrefetchSections(
                        tx.LowLevelTransaction.RootObjects,
                        tx.LowLevelTransaction);

                    tx.CreateTree(DocsSlice);
                    tx.CreateTree(LastReplicatedEtagsSlice);
                    tx.CreateTree(ChangeVectorSlice);

                    CollectionsSchema.Create(tx, CollectionsSlice, 32);

                    Identities = new IdentitiesStorage(_documentDatabase, tx);
                    ConflictsStorage = new ConflictsStorage(_documentDatabase, tx);
                    AttachmentsStorage = new AttachmentsStorage(_documentDatabase, tx);
                    DocumentPut = new DocumentPutAction(this, _documentDatabase);

                    _lastEtag = ReadLastEtag(tx);
                    _collectionsCache = ReadCollections(tx);

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not open server store for " + _name, e);

                Dispose();
                options.Dispose();
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

            var tree = context.Transaction.InnerTransaction.ReadTree(ChangeVectorSlice);
            return ChangeVectorUtils.ReadChangeVectorFrom(tree);
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, Dictionary<Guid, long> changeVector)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(ChangeVectorSlice);
            ChangeVectorUtils.WriteChangeVectorTo(context, changeVector, tree);
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, ChangeVectorEntry[] changeVector)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(ChangeVectorSlice);
            ChangeVectorUtils.WriteChangeVectorTo(context, changeVector, tree);
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
            return ReadLastEtagFrom(tx, ConflictsStorage.AllConflictedDocsEtagsSlice);
        }

        public static long ReadLastRevisionsEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, VersioningStorage.RevisionsEtagsSlice);
        }

        public static long ReadLastAttachmentsEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, AttachmentsStorage.AttachmentsEtagSlice);
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
            var tree = tx.CreateTree(EtagsSlice);
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

            var lastAttachmentEtag = ReadLastAttachmentsEtag(tx);
            if (lastAttachmentEtag > lastEtag)
                lastEtag = lastAttachmentEtag;

            return lastEtag;
        }

        public static long ComputeEtag(long etag, long numberOfDocuments)
        {
            var buffer = stackalloc long[2];
            buffer[0] = etag;
            buffer[1] = numberOfDocuments;
            return (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * 2);
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string idPrefix, string matches, string exclude, string startAfterId, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            var isStartAfter = string.IsNullOrWhiteSpace(startAfterId) == false;

            Slice prefixSlice;
            var startAfterSlice = Slices.Empty;
            using (DocumentKeyWorker.GetSliceFromKey(context, idPrefix, out prefixSlice))
            using (isStartAfter ? (IDisposable)DocumentKeyWorker.GetSliceFromKey(context, startAfterId, out startAfterSlice) : null)
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(prefixSlice, startAfterSlice, 0))
                {
                    var document = TableValueToDocument(context, ref result.Reader);
                    string documentKey = document.Key;
                    if (documentKey.StartsWith(idPrefix, StringComparison.OrdinalIgnoreCase) == false)
                        break;

                    var keyTest = documentKey.Substring(idPrefix.Length);
                    if (WildcardMatcher.Matches(matches, keyTest) == false || WildcardMatcher.MatchesExclusion(exclude, keyTest))
                        continue;

                    if (start > 0)
                    {
                        start--;
                        continue;
                    }

                    if (take-- <= 0)
                        yield break;

                    yield return document;
                }
            }
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFromLast(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice]))
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
                collectionName.GetTableName(CollectionTableType.Documents));

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFromLast(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice]))
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
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                {
                    yield break;
                }

                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<ReplicationBatchItem> GetDocumentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchItem.From(TableValueToDocument(context, ref result.Reader));
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
                collectionName.GetTableName(CollectionTableType.Documents));

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<(ChangeVectorEntry[], long)> GetChangeVectorsFrom(DocumentsOperationContext context, string collection, long etag, int start, int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents));

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                var curEtag = TableValueToEtag((int)DocumentsTable.Etag, ref result.Reader);
                var curChangeVector = GetChangeVectorEntriesFromTableValueReader(ref result.Reader, (int)DocumentsTable.ChangeVector);

                yield return (curChangeVector,curEtag);
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

        public DocumentOrTombstone GetDocumentOrTombstone(DocumentsOperationContext context, string key, bool throwOnConflict = true)
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

        public struct DocumentOrTombstone
        {
            public Document Document;
            public DocumentTombstone Tombstone;
            public bool Missing => Document == null && Tombstone == null;
        }

        public DocumentOrTombstone GetDocumentOrTombstone(DocumentsOperationContext context, Slice loweredKey, bool throwOnConflict = true)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return default(DocumentOrTombstone);// never hit
            }

            try
            {
                var doc = Get(context, loweredKey);
                if (doc != null)
                    return new DocumentOrTombstone { Document = doc };
            }
            catch (DocumentConflictException)
            {
                if (throwOnConflict)
                    throw;
                return new DocumentOrTombstone();
            }

            var tombstoneTable = new Table(TombstonesSchema, context.Transaction.InnerTransaction);
            TableValueReader tvr;
            tombstoneTable.ReadByKey(loweredKey, out tvr);

            return new DocumentOrTombstone
            {
                Tombstone = TableValueToTombstone(context, ref tvr)
            };
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
            TableValueReader tvr;
            if (GetTableValueReaderForDocument(context, loweredKey, out tvr) == false)
                return null;

            var doc = TableValueToDocument(context, ref tvr);

            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(doc);

            return doc;
        }

        public bool GetTableValueReaderForDocument(DocumentsOperationContext context, Slice loweredKey,
            out TableValueReader tvr)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(loweredKey, out tvr) == false)
            {
                if (ConflictsStorage.ConflictsCount > 0)
                    ConflictsStorage.ThrowOnDocumentConflict(context, loweredKey);
                return false;
            }
            return true;
        }

        public bool HasMoreOfTombstonesAfter(
            DocumentsOperationContext context,
            long etag,
            int maxAllowed)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var _ in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
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
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public IEnumerable<ReplicationBatchItem> GetTombstonesFrom(
            DocumentsOperationContext context,
            long etag)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchItem.From(TableValueToTombstone(context, ref result.Reader));
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
                collectionName.GetTableName(CollectionTableType.Tombstones));

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, start))
            {
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
                collectionName.GetTableName(CollectionTableType.Documents)
                );

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice]);
            if (result == null)
                return 0;

            int size;
            var ptr = result.Reader.Read((int)DocumentsTable.Etag, out size);
            return IPAddress.NetworkToHostOrder(*(long*)ptr);
        }

        public long GetLastTombstoneEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones));

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice]);
            if (result == null)
                return 0;

            return TableValueToEtag(1, ref result.Reader);
        }

        public long GetNumberOfTombstonesWithDocumentEtagLowerThan(DocumentsOperationContext context, string collection, long etag)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones));

            if (table == null)
                return 0;

            return table.CountBackwardFrom(TombstonesSchema.FixedSizeIndexes[DeletedEtagsSlice], etag);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Document TableValueToDocument(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var document = ParseDocument(context, ref tvr);
            DebugDisposeReaderAfterTransction(context.Transaction, document.Data);
            DocumentPutAction.AssertMetadataWasFiltered(document.Data);
            return document;
        }

        [Conditional("DEBUG")]
        public static void DebugDisposeReaderAfterTransction(DocumentsTransaction tx, BlittableJsonReaderObject reader)
        {
            if (reader == null)
                return;
            Debug.Assert(tx != null);
            // this method is called to ensure that after the transaction is completed, all the readers are disposed
            // so we won't have read-after-tx use scenario, which can in rare case corrupt memory. This is a debug
            // helper that is used across the board, but it is meant to assert stuff during debug only
            tx.InnerTransaction.LowLevelTransaction.OnDispose += state => reader.Dispose();
        }

        public static Document ParseDocument(JsonOperationContext context, ref TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id,
                LoweredKey = TableValueToString(context, (int)DocumentsTable.LoweredKey, ref tvr),
                Key = TableValueToKey(context, (int)DocumentsTable.Key, ref tvr),
                Etag = TableValueToEtag((int)DocumentsTable.Etag, ref tvr)
            };

            int size;
            result.Data = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out size), size, context);
            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)DocumentsTable.ChangeVector);
            result.LastModified = new DateTime(*(long*)tvr.Read((int)DocumentsTable.LastModified, out size));
            result.Flags = *(DocumentFlags*)tvr.Read((int)DocumentsTable.Flags, out size);

            result.TransactionMarker = *(short*)tvr.Read((int)DocumentsTable.TransactionMarker, out size);

            return result;
        }

        public static ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(ref TableValueReader tvr, int index)
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

        private static DocumentTombstone TableValueToTombstone(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            if (tvr.Pointer == null)
                return null;

            var result = new DocumentTombstone
            {
                StorageId = tvr.Id,
                LoweredKey = TableValueToString(context, (int)TombstoneTable.LoweredKey, ref tvr),
                Etag = TableValueToEtag((int)TombstoneTable.Etag, ref tvr),
                DeletedEtag = TableValueToEtag((int)TombstoneTable.DeletedEtag, ref tvr)
            };

            int size;
            result.Type = *(DocumentTombstone.TombstoneType*)tvr.Read((int)TombstoneTable.Type, out size);
            result.TransactionMarker = *(short*)tvr.Read((int)TombstoneTable.TransactionMarker, out size);

            if (result.Type == DocumentTombstone.TombstoneType.Document)
            {
                result.Collection = TableValueToString(context, (int)TombstoneTable.Collection, ref tvr);
                result.Flags = *(DocumentFlags*)tvr.Read((int)TombstoneTable.Flags, out size);
                result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)TombstoneTable.ChangeVector);
                result.LastModified = new DateTime(*(long*)tvr.Read((int)TombstoneTable.LastModified, out size));
            }

            return result;
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, string key, long? expectedEtag)
        {
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out Slice loweredKey))
            {
                return Delete(context, loweredKey, key, expectedEtag);
            }
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context,
            Slice loweredKey,
            string key, // TODO: Should be a LazyStringValue
            long? expectedEtag,
            long? lastModifiedTicks = null,
            ChangeVectorEntry[] changeVector = null,
            LazyStringValue collection = null)
        {
            var collectionName = collection != null ? new CollectionName(collection) : null;

            if (ConflictsStorage.ConflictsCount != 0)
            {
                var result = ConflictsStorage.DeleteConflicts(context, loweredKey, expectedEtag, changeVector);
                if (result != null)
                    return result;
            }

            var local = GetDocumentOrTombstone(context, loweredKey, throwOnConflict: false);

            if (local.Tombstone != null)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException($"Document {local.Tombstone.LoweredKey} does not exist, but delete was called with etag {expectedEtag}. " +
                                                   $"Optimistic concurrency violation, transaction will be aborted.");

                collectionName = ExtractCollectionName(context, local.Tombstone.Collection);

                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, 
                    collectionName.GetTableName(CollectionTableType.Tombstones));
                tombstoneTable.Delete(local.Tombstone.StorageId);

                // we update the tombstone
                var etag = CreateTombstone(context,
                    loweredKey,
                    local.Tombstone.Etag,
                    collectionName,
                    local.Tombstone.ChangeVector,
                    lastModifiedTicks,
                    changeVector,
                    DocumentFlags.None);

                // We have to raise the notification here because even though we have deleted
                // a deleted value, we changed the change vector. And maybe we need to replicate 
                // that. Another issue is that the last tombstone etag has changed, and we need 
                // to let the indexes catch up to us here, even if they'll just do a noop.

                // TODO: Do not send here strings. Use lazy strings instead.
                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    Type = DocumentChangeTypes.Delete,
                    Etag = etag,
                    Key = key,
                    CollectionName = collectionName.Name,
                    IsSystemDocument = collectionName.IsSystem,
                });

                return new DeleteOperationResult
                {
                    Collection = collectionName,
                    Etag = etag
                };
            }

            if (local.Document != null)
            {
                // just delete the document
                var doc = local.Document;
                if (expectedEtag != null && doc.Etag != expectedEtag)
                {
                    throw new ConcurrencyException(
                        $"Document {loweredKey} has etag {doc.Etag}, but Delete was called with etag {expectedEtag}. " +
                        $"Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ActualETag = doc.Etag,
                        ExpectedETag = (long)expectedEtag
                    };
                }

                EnsureLastEtagIsPersisted(context, doc.Etag);

                collectionName = ExtractCollectionName(context, loweredKey, doc.Data);
                var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                int size;
                var ptr = table.DirectRead(doc.StorageId, out size);
                var tvr = new TableValueReader(ptr, size);
                var flags = *(DocumentFlags*)tvr.Read((int)DocumentsTable.Flags, out size);

                byte* lowerKeyPtr = tvr.Read((int)DocumentsTable.LoweredKey, out size);
                Slice tombstone;
                Slice.External(context.Allocator, lowerKeyPtr, size, out tombstone);

                var etag = CreateTombstone(context, tombstone, doc.Etag, collectionName, doc.ChangeVector, lastModifiedTicks, changeVector, doc.Flags);

                if (collectionName.IsSystem == false &&
                    (flags & DocumentFlags.Versioned) == DocumentFlags.Versioned)
                {
                    _documentDatabase.BundleLoader.VersioningStorage?.Delete(context, collectionName, loweredKey);
                }
                table.Delete(doc.StorageId);

                if ((flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                    AttachmentsStorage.DeleteAttachmentsOfDocument(context, loweredKey);

                // TODO: Do not send here strings. Use lazy strings instead.
                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    Type = DocumentChangeTypes.Delete,
                    Etag = etag,
                    Key = key,
                    CollectionName = collectionName.Name,
                    IsSystemDocument = collectionName.IsSystem,
                });

                return new DeleteOperationResult
                {
                    Collection = collectionName,
                    Etag = etag
                };
            }
            else
            {
                // we adding a tombstone without having any pervious document, it could happened if this was called
                // from the incoming replication or if we delete document that wasn't exist at the first place.
                if (expectedEtag != null)
                    throw new ConcurrencyException($"Document {loweredKey} does not exist, but delete was called with etag {expectedEtag}. " +
                                                   $"Optimistic concurrency violation, transaction will be aborted.");

                if (collectionName == null)
                {
                    // this basically mean that we tried to delete document that doesn't exist.
                    return null;
                }

                // ensures that the collection trees will be created
                collectionName = ExtractCollectionName(context, collectionName.Name);

                var etag = CreateTombstone(context,
                    loweredKey,
                    -1, // delete etag is not relevant
                    collectionName,
                    changeVector,
                    DateTime.UtcNow.Ticks,
                    null,
                    DocumentFlags.None);

                return new DeleteOperationResult
                {
                    Collection = collectionName,
                    Etag = etag
                };
            }
        }

        // Note: Make sure to call this with a seprator, to you won't delete "users/11" for "users/1"
        public List<DeleteOperationResult> DeleteDocumentsStartingWith(DocumentsOperationContext context, string prefix)
        {
            var deleteResults = new List<DeleteOperationResult>();

            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            Slice prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, prefix, out prefixSlice))
            {
                bool hasMore = true;
                while (hasMore)
                {
                    hasMore = false;

                    foreach (var holder in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                    {
                        hasMore = true;
                        var key = TableValueToKey(context, (int)DocumentsTable.Key, ref holder.Reader);

                        var deleteOperationResult = Delete(context, key, null);
                        if (deleteOperationResult != null)
                            deleteResults.Add(deleteOperationResult.Value);
                    }

                }

            }

            return deleteResults;
        }

        public struct DeleteOperationResult
        {
            public long Etag;
            public CollectionName Collection;
        }

        public long GenerateNextEtag()
        {
            return ++_lastEtag;
        }

        public void EnsureLastEtagIsPersisted(DocumentsOperationContext context, long docEtag)
        {
            if (docEtag != _lastEtag)
                return;
            var etagTree = context.Transaction.InnerTransaction.ReadTree(EtagsSlice);
            var etag = _lastEtag;
            Slice etagSlice;
            using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
                etagTree.Add(LastEtagSlice, etagSlice);
        }

        public long CreateTombstone(
            DocumentsOperationContext context,
            Slice lowerKey,
            long documentEtag,
            CollectionName collectionName,
            ChangeVectorEntry[] docChangeVector,
            long? lastModifiedTicks,
            ChangeVectorEntry[] changeVector,
            DocumentFlags flags)
        {
            var newEtag = GenerateNextEtag();

            if (changeVector == null)
            {
                changeVector = ConflictsStorage.GetMergedConflictChangeVectorsAndDeleteConflicts(
                    context,
                    lowerKey,
                    newEtag,
                    docChangeVector);
            }
            else
            {
                ConflictsStorage.DeleteConflictsFor(context, lowerKey);
            }

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                Slice collectionSlice;
                using (Slice.From(context.Allocator, collectionName.Name, out collectionSlice))
                {
                    var modifiedTicks = lastModifiedTicks ?? _documentDatabase.Time.GetUtcNow().Ticks;

                    var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                        collectionName.GetTableName(CollectionTableType.Tombstones));
                    TableValueBuilder tbv;
                    using (table.Allocate(out tbv))
                    {
                        tbv.Add(lowerKey);
                        tbv.Add(Bits.SwapBytes(newEtag));
                        tbv.Add(Bits.SwapBytes(documentEtag));
                        tbv.Add(context.GetTransactionMarker());
                        tbv.Add((byte)DocumentTombstone.TombstoneType.Document);
                        tbv.Add(collectionSlice);
                        tbv.Add((int)flags);
                        tbv.Add((byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVector.Length);
                        tbv.Add(modifiedTicks);

                        table.Insert(tbv);
                    }
                }
            }
            return newEtag;
        }

        public struct PutOperationResults
        {
            public string Key;
            public long Etag;
            public CollectionName Collection;
            public DateTime LastModified;
            public ChangeVectorEntry[] ChangeVector;
            public DocumentFlags Flags;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PutOperationResults Put(DocumentsOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document,
            long? lastModifiedTicks = null,
            ChangeVectorEntry[] changeVector = null,
            DocumentFlags flags = DocumentFlags.None,
            NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            return DocumentPut.PutDocument(context, key, expectedEtag, document, lastModifiedTicks, changeVector, flags, nonPersistentFlags);
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
                    collectionName.GetTableName(CollectionTableType.Tombstones));

                indexDef = TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }
            else
            {
                table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                    collectionName.GetTableName(CollectionTableType.Documents));
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
                collectionName.GetTableName(CollectionTableType.Documents));

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

        public void DeleteTombstonesBefore(string collection, long etag, DocumentsOperationContext context)
        {
            string tableName;

            if (collection == AttachmentsStorage.AttachmentsTombstones)
            {
                tableName = collection;
            }
            else
            {
                var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    return;

                tableName = collectionName.GetTableName(CollectionTableType.Tombstones);
            }

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, tableName);
            if (table == null)
                return;

            var deleteCount = table.DeleteBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, long.MaxValue);
            if (_logger.IsInfoEnabled && deleteCount > 0)
                _logger.Info($"Deleted {deleteCount:#,#;;0} tombstones earlier than {etag} in {collection}");

        }

        public IEnumerable<string> GetTombstoneCollections(Transaction transaction)
        {
            yield return AttachmentsStorage.AttachmentsTombstones;

            using (var it = transaction.LowLevelTransaction.RootObjects.Iterate(false))
            {
                it.SetRequiredPrefix(TombstonesPrefix);

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

        public long GetLastReplicateEtagFrom(DocumentsOperationContext context, string dbId)
        {
            var readTree = context.Transaction.InnerTransaction.ReadTree(LastReplicatedEtagsSlice);
            var readResult = readTree.Read(dbId);
            if (readResult == null)
                return 0;
            return readResult.Reader.ReadLittleEndianInt64();
        }

        public void SetLastReplicateEtagFrom(DocumentsOperationContext context, string dbId, long etag)
        {
            var etagsTree = context.Transaction.InnerTransaction.CreateTree(LastReplicatedEtagsSlice);
            Slice etagSlice;
            Slice keySlice;
            using (Slice.From(context.Allocator, dbId, out keySlice))
            using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out etagSlice))
            {
                etagsTree.Add(keySlice, etagSlice);
            }
        }

        public CollectionName GetCollection(string collection, bool throwIfDoesNotExist)
        {
            CollectionName collectionName;
            if (_collectionsCache.TryGetValue(collection, out collectionName) == false && throwIfDoesNotExist)
                throw new InvalidOperationException($"There is no collection for '{collection}'.");

            return collectionName;
        }

        public CollectionName ExtractCollectionName(DocumentsOperationContext context, string key, BlittableJsonReaderObject document)
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

            var collections = context.Transaction.InnerTransaction.OpenTable(CollectionsSchema, CollectionsSlice);

            name = new CollectionName(collectionName);

            Slice collectionSlice;
            using (Slice.From(context.Allocator, collectionName, out collectionSlice))
            {
                TableValueBuilder tvr;
                using (collections.Allocate(out tvr))
                {
                    tvr.Add(collectionSlice);
                    collections.Set(tvr);
                }

                DocsSchema.Create(context.Transaction.InnerTransaction, name.GetTableName(CollectionTableType.Documents), 16);
                TombstonesSchema.Create(context.Transaction.InnerTransaction,
                    name.GetTableName(CollectionTableType.Tombstones), 16);

                // Add to cache ONLY if the transaction was committed. 
                // this would prevent NREs next time a PUT is run,since if a transaction
                // is not commited, DocsSchema and TombstonesSchema will not be actually created..
                // has to happen after the commit, but while we are holding the write tx lock
                context.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ =>
                {
                    var collectionNames = new FastDictionary<string, CollectionName, OrdinalIgnoreCaseStringStructComparer>(_collectionsCache, OrdinalIgnoreCaseStringStructComparer.Instance)
                    {
                        [name.Name] = name
                    };
                    _collectionsCache = collectionNames;
                };
            }
            return name;
        }

        private FastDictionary<string, CollectionName, OrdinalIgnoreCaseStringStructComparer> ReadCollections(Transaction tx)
        {
            var result = new FastDictionary<string, CollectionName, OrdinalIgnoreCaseStringStructComparer>(OrdinalIgnoreCaseStringStructComparer.Instance);

            var collections = tx.OpenTable(CollectionsSchema, CollectionsSlice);

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                foreach (var tvr in collections.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                {
                    var collection = TableValueToString(context, 0, ref tvr.Reader);
                    var collectionName = new CollectionName(collection);
                    result.Add(collection, collectionName);

                    var documentsTree = tx.ReadTree(collectionName.GetTableName(CollectionTableType.Documents), RootObjectType.Table);
                    NewPageAllocator.MaybePrefetchSections(documentsTree, tx.LowLevelTransaction);

                    var tombstonesTree = tx.ReadTree(collectionName.GetTableName(CollectionTableType.Tombstones), RootObjectType.Table);
                    NewPageAllocator.MaybePrefetchSections(tombstonesTree, tx.LowLevelTransaction);
                }
            }

            return result;
        }

        public void UpdateDocumentAfterAttachmentChange(DocumentsOperationContext context, Slice lowerDocumentId, string documentId, TableValueReader tvr)
        {
            // We can optimize this by copy just the document's data instead of the all tvr
            var copyOfDoc = context.GetMemory(tvr.Size);
            try
            {
                // we have to copy it to the side because we might do a defrag during update, and that
                // can cause corruption if we read from the old value (which we just deleted)
                Memory.Copy(copyOfDoc.Address, tvr.Pointer, tvr.Size);
                var copyTvr = new TableValueReader(copyOfDoc.Address, tvr.Size);
                int size;
                var data = new BlittableJsonReaderObject(copyTvr.Read((int)DocumentsTable.Data, out size), size, context);

                var attachments = AttachmentsStorage.GetAttachmentsMetadataForDocument(context, lowerDocumentId);

                var flags = DocumentFlags.None;
                data.Modifications = new DynamicJsonValue(data);
                if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);

                    if (attachments.Count > 0)
                    {
                        flags = DocumentFlags.HasAttachments;
                        metadata.Modifications[Constants.Documents.Metadata.Attachments] = attachments;
                    }
                    else
                    {
                        metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);
                    }

                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
                else
                {
                    if (attachments.Count > 0)
                    {
                        flags = DocumentFlags.HasAttachments;
                        data.Modifications[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Attachments] = attachments
                        };
                    }
                    else
                    {
                        Debug.Assert(false, "Cannot remove an attachment and not have @attachments in @metadata");
                    }
                }

                data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                Put(context, documentId, null, data, null, null, flags, NonPersistentDocumentFlags.ByAttachmentUpdate);
            }
            finally
            {
                context.ReturnMemory(copyOfDoc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long TableValueToEtag(int index, ref TableValueReader tvr)
        {
            int size;
            var ptr = tvr.Read(index, out size);
            var etag = Bits.SwapBytes(*(long*)ptr);
            return etag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LazyStringValue TableValueToString(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            int size;
            var ptr = tvr.Read(index, out size);
            return context.AllocateStringValue(null, ptr, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LazyStringValue TableValueToKey(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            byte offset;
            var ptr = tvr.Read(index, out size);
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            return context.AllocateStringValue(null, ptr + offset, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext<ByteStringMemoryCache>.ExternalScope TableValueToSlice(
            DocumentsOperationContext context, int index, ref TableValueReader tvr, out Slice slice)
        {
            int size;
            var ptr = tvr.Read(index, out size);
            return Slice.External(context.Allocator, ptr, size, out slice);
        }
    }
}