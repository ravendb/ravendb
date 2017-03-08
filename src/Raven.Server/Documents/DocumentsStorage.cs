using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Versioning;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
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
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private static readonly Slice KeySlice;

        private static readonly Slice DocsSlice;
        private static readonly Slice CollectionEtagsSlice;
        private static readonly Slice AllDocsEtagsSlice;
        private static readonly Slice TombstonesSlice;
        private static readonly Slice CollectionsSlice;
        private static readonly Slice KeyAndChangeVectorSlice;
        private static readonly Slice AllConflictedDocsEtagsSlice;
        private static readonly Slice ConflictsSlice;
        private static readonly Slice IdentitiesSlice;
        private static readonly Slice LastReplicatedEtagsSlice;
        private static readonly Slice ChangeVectorSlice;
        private static readonly Slice ConflictedCollectionSlice;
        private static readonly Slice EtagsSlice;
        private static readonly Slice LastEtagSlice;
        private static readonly Slice AttachmentsSlice;
        private static readonly Slice AttachmentsMetadataSlice;
        private static readonly Slice AttachmentsEtagSlice;
        private static readonly Slice AttachmentsHashSlice;

        private static readonly Slice AllTombstonesEtagsSlice;
        public static readonly TableSchema DocsSchema = new TableSchema();
        private static readonly Slice TombstonesPrefix;
        private static readonly Slice DeletedEtagsSlice;
        private static readonly TableSchema ConflictsSchema = new TableSchema();
        private static readonly TableSchema TombstonesSchema = new TableSchema();
        private static readonly TableSchema CollectionsSchema = new TableSchema();
        private static readonly TableSchema AttachmentsSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;

        private Dictionary<string, CollectionName> _collectionsCache;

        private enum ConflictsTable
        {
            LoweredKey = 0,
            ChangeVector = 1,
            OriginalKey = 2,
            Data = 3,
            Etag = 4,
            Collection = 5,
            LastModified = 6,
        }
        private enum TombstoneTable
        {
            LoweredKey = 0,
            Etag = 1,
            DeletedEtag = 2,
            Key = 3,
            ChangeVector = 4,
            Collection = 5,
            Flags = 6,
            TransactionMarker = 7,
            LastModified = 8,
        }

        // The attachments schema is as follows
        // 5 fields (lowered document id AND record separator AND lowered name, etag, name, content type, last modified)
        // We are you using the record separator in order to avoid loading another files that has the same key prefix, 
        //      e.g. fitz(record-separator)profile.png and fitz0(record-separator)profile.png, without the record separator we would have to load also fitz0 and filter it.
        // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
        private enum AttachmentsTable
        {
            LoweredDocumentIdAndRecordSeparatorAndLoweredName = 0,
            Etag = 1,
            Name = 2,
            ContentType = 3,
            Hash = 4,
            LastModified = 5,
        }

        private enum DocumentsTable
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
            Slice.From(StorageEnvironment.LabelsContext, "Key", ByteStringType.Immutable, out KeySlice);
            Slice.From(StorageEnvironment.LabelsContext, "Docs", ByteStringType.Immutable, out DocsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllConflictedDocsEtags", ByteStringType.Immutable, out AllConflictedDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Tombstones", ByteStringType.Immutable, out TombstonesSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Collections", ByteStringType.Immutable, out CollectionsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndChangeVector", ByteStringType.Immutable, out KeyAndChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, CollectionName.GetTablePrefix(CollectionTableType.Tombstones), ByteStringType.Immutable, out TombstonesPrefix);
            Slice.From(StorageEnvironment.LabelsContext, "DeletedEtags", ByteStringType.Immutable, out DeletedEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Conflicts", ByteStringType.Immutable, out ConflictsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Identities", ByteStringType.Immutable, out IdentitiesSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastReplicatedEtags", ByteStringType.Immutable, out LastReplicatedEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "ChangeVector", ByteStringType.Immutable, out ChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "ConflictedCollection", ByteStringType.Immutable, out ConflictedCollectionSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Attachments", ByteStringType.Immutable, out AttachmentsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AttachmentsMetadata", ByteStringType.Immutable, out AttachmentsMetadataSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AttachmentsEtag", ByteStringType.Immutable, out AttachmentsEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AttachmentsHash", ByteStringType.Immutable, out AttachmentsHashSlice);

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
                Name = AllConflictedDocsEtagsSlice
            });
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.Collection,
                Count = 1,
                IsGlobal = true,
                Name = ConflictedCollectionSlice
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

            AttachmentsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.LoweredDocumentIdAndRecordSeparatorAndLoweredName,
                Count = 1,
            });
            AttachmentsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.Etag,
                Name = AttachmentsEtagSlice
            });
            AttachmentsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.Hash,
                Name = AttachmentsHashSlice
            });
        }

        private readonly Logger _logger;
        private readonly string _name;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        public DocumentsContextPool ContextPool;

        private long _conflictCount;
        public long ConflictsCount => _conflictCount;

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
                ? StorageEnvironmentOptions.CreateMemoryOnly(_documentDatabase.Configuration.Core.DataDirectory.FullPath, null, _documentDatabase.IoChanges)
                : StorageEnvironmentOptions.ForPath(
                    _documentDatabase.Configuration.Core.DataDirectory.FullPath,
                    _documentDatabase.Configuration.Storage.TempPath?.FullPath,
                    _documentDatabase.Configuration.Storage.JournalsStoragePath?.FullPath,
                    _documentDatabase.IoChanges
                    );

            options.ForceUsing32BitsPager = _documentDatabase.Configuration.Storage.ForceUsing32BitsPager;

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
            options.SchemaVersion = 4;
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
                    tx.CreateTree(IdentitiesSlice);
                    tx.CreateTree(ChangeVectorSlice);
                    tx.CreateTree(AttachmentsSlice);

                    ConflictsSchema.Create(tx, ConflictsSlice, 32);
                    CollectionsSchema.Create(tx, CollectionsSlice, 32);
                    AttachmentsSchema.Create(tx, AttachmentsMetadataSlice, 32);

                    _conflictCount = tx.OpenTable(ConflictsSchema, ConflictsSlice).NumberOfEntries;

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
            return ReplicationUtils.ReadChangeVectorFrom(tree);
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, Dictionary<Guid, long> changeVector)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(ChangeVectorSlice);
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
            return ReadLastEtagFrom(tx, AllConflictedDocsEtagsSlice);
        }

        public static long ReadLastRevisionsEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, VersioningStorage.RevisionsEtagsSlice);
        }

        public static long ReadLastAttachmentsEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, AttachmentsEtagSlice);
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
                foreach (var result in table.SeekByPrimaryKeyStartingWith(prefixSlice, startAfterSlice, 0))
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
                collectionName.GetTableName(CollectionTableType.Documents));

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
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                {
                    yield break;
                }

                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<ReplicationBatchDocumentItem> GetDocumentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchDocumentItem.From(TableValueToDocument(context, ref result.Reader));
            }
        }

        public IEnumerable<ReplicationBatchDocumentItem> GetAttachmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            foreach (var result in table.SeekForwardFrom(AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice], etag, 0))
            {
                yield return ReplicationBatchDocumentItem.From(TableValueToAttachment(context, ref result.Reader));
            }
        }

        private long GetCountOfAttachmentsForHash(DocumentsOperationContext context, Slice hash)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            return table.GetCountOfMatchesFor(AttachmentsSchema.Indexes[AttachmentsHashSlice], hash);
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

        public List<DocumentConflict> GetAllConflictsBySameKeyAfter(DocumentsOperationContext context, ref Slice lastKey)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            var list = new List<DocumentConflict>();
            LazyStringValue firstKey = null;
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], lastKey, 0))
            {
                var conflict = TableValueToConflictDocument(context, ref tvr.Result.Reader);
                if (lastKey.Content.Match(conflict.LoweredKey))
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

        public IEnumerable<ReplicationBatchDocumentItem> GetConflictsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchDocumentItem.From(TableValueToConflictDocument(context, ref tvr.Reader));
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
                ThrowRequiresTransaction();
            Debug.Assert(context.Transaction != null);

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
            TableValueReader tvr;
            if (GetTableValueReaderForDocument(context, loweredKey, out tvr) == false)
                return null;

            var doc = TableValueToDocument(context, ref tvr);

            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(doc);

            return doc;
        }

        private bool GetTableValueReaderForDocument(DocumentsOperationContext context, Slice loweredKey,
            out TableValueReader tvr)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(loweredKey, out tvr) == false)
            {
                if (_conflictCount > 0)
                    ThrowOnDocumentConflict(context, loweredKey);
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
            foreach (
                var _ in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
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

        public IEnumerable<ReplicationBatchDocumentItem> GetTombstonesFrom(
            DocumentsOperationContext context,
            long etag)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchDocumentItem.From(TableValueToTombstone(context, ref result.Reader));
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

            var result = table
                        .SeekBackwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue)
                        .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Reader.Read((int) DocumentsTable.Etag, out size);
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

            var result = table
                .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue)
                .FirstOrDefault();

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

            return table
                    .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[DeletedEtagsSlice], etag)
                    .Count();
        }

        private Document TableValueToDocument(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var document = ParseDocument(context, ref tvr);
            DebugDisposeReaderAfterTransction(context.Transaction, document.Data);

            if ((document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                Slice startSlice;
                using (GetAttachmentPrefix(context, document.LoweredKey.Buffer, document.LoweredKey.Size, out startSlice))
                {
                    document.Attachments = GetAttachmentsForDocument(context, startSlice);
                }
            }

            return document;
        }

        [Conditional("DEBUG")]
        private static void DebugDisposeReaderAfterTransction(DocumentsTransaction tx, BlittableJsonReaderObject reader)
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
                StorageId = tvr.Id
            };
            result.LoweredKey = TableValueToString(context, (int)DocumentsTable.LoweredKey, ref tvr);
            result.Key = TableValueToKey(context, (int)DocumentsTable.Key, ref tvr);
            result.Etag = TableValueToEtag((int)DocumentsTable.Etag, ref tvr);

            int size;
            result.Data = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out size), size, context);
            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)DocumentsTable.ChangeVector);
            result.LastModified = new DateTime(*(long*)tvr.Read((int)DocumentsTable.LastModified, out size));
            result.Flags = *(DocumentFlags*)tvr.Read((int)DocumentsTable.Flags, out size);

            result.TransactionMarker = *(short*)tvr.Read((int)DocumentsTable.TransactionMarker, out size);

            return result;
        }

        private static DocumentConflict TableValueToConflictDocument(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new DocumentConflict
            {
                StorageId = tvr.Id
            };
            
            result.LoweredKey = TableValueToString(context, (int)ConflictsTable.LoweredKey, ref tvr);
            result.Key = TableValueToKey(context, (int)ConflictsTable.OriginalKey, ref tvr);
            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)ConflictsTable.ChangeVector);
            result.Etag = TableValueToEtag((int)ConflictsTable.Etag, ref tvr);
            result.Collection = TableValueToString(context, (int)ConflictsTable.Collection, ref tvr);

            int size;
            var read = tvr.Read((int)ConflictsTable.Data, out size);
            if (size > 0)
            {
                //otherwise this is a tombstone conflict and should be treated as such
                result.Doc = new BlittableJsonReaderObject(read, size, context);
                DebugDisposeReaderAfterTransction(context.Transaction, result.Doc);
            }

            result.LastModified = new DateTime(*(long*)tvr.Read((int)ConflictsTable.LastModified, out size));

            return result;
        }

        private static ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(ref TableValueReader tvr, int index)
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
            if (tvr.Pointer == null)
                return null;

            var result = new DocumentTombstone
            {
                StorageId = tvr.Id
            };
            result.LoweredKey = TableValueToString(context, (int)TombstoneTable.LoweredKey, ref tvr);
            result.Key = TableValueToKey(context, (int)TombstoneTable.Key, ref tvr);
            result.Etag = TableValueToEtag((int)TombstoneTable.Etag, ref tvr);
            result.DeletedEtag = TableValueToEtag((int)TombstoneTable.DeletedEtag, ref tvr);
            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)TombstoneTable.ChangeVector);
            result.Collection = TableValueToString(context, (int)TombstoneTable.Collection, ref tvr);

            int size;
            result.Flags = *(DocumentFlags*)tvr.Read((int)TombstoneTable.Flags, out size);
            result.TransactionMarker = *(short*)tvr.Read((int)TombstoneTable.TransactionMarker, out size);
            result.LastModified = new DateTime(*(long*)tvr.Read((int)TombstoneTable.LastModified, out size));

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

        public DeleteOperationResult? Delete(DocumentsOperationContext context,
            Slice loweredKey,
            string key,
            long? expectedEtag,
            long? lastModifiedTicks = null,
            ChangeVectorEntry[] changeVector = null,
            LazyStringValue collection = null)
        {
            var local = GetDocumentOrTombstone(context, loweredKey, throwOnConflict: false);
            var currentLastTicks = DateTime.UtcNow.Ticks;
            long etag = -1;
            var collectionName = collection != null ? new CollectionName(collection) : null;

            if (_conflictCount > 0)
            {
                var conflicts = GetConflictsFor(context, loweredKey);
                if (conflicts.Count > 0) //we do have a conflict for our deletion candidate
                {
                    // Since this document resolve the conflict we dont need to alter the change vector.
                    // This way we avoid another replication back to the source
                    if (expectedEtag.HasValue)
                    {
                        long currentMaxConflictEtag;
                        currentMaxConflictEtag = GetConflictsMaxEtagFor(context, loweredKey);

                        ThrowConcurrencyExceptionOnConflict(expectedEtag, currentMaxConflictEtag);
                    }

                    if (local.Item2 != null || local.Item1 != null)
                    {
                        // Something is wrong, we can't have conflicts and local document/tombstone
                        ThrowInvalidConflictWithTombstone(loweredKey);
                    }
                    collectionName = ResolveConflictAndAddTombstone(context, changeVector, conflicts, out etag);
                }
            }
            else
            {
                byte* lowerKey;
                int lowerSize;
                byte* keyPtr;
                int keySize;

                if (key != null)
                {
                    DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key,
                        out lowerKey, out lowerSize, out keyPtr, out keySize);

                    if (local.Item2 == null && local.Item1 == null)
                    {
                        // we adding a tombstone without having any pervious document, it could happened if this was called
                        // from the incoming replication or if we delete document that wasn't exist at the first place.

                        if (expectedEtag != null)
                            throw new ConcurrencyException(
                                $"Document {key} does not exist, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                        if (collectionName == null)
                        {
                            // this basically mean that we tried to delete document that doesn't exist.
                            return null;
                        }

                        etag = CreateTombstone(context,
                            lowerKey,
                            lowerSize,
                            keyPtr,
                            keySize,
                            -1, // delete etag is not relevant
                            collectionName,
                            changeVector,
                            currentLastTicks,
                            null,
                            DocumentFlags.None);
                    }

                    if (local.Item2 != null)
                    {
                        if (expectedEtag != null)
                            throw new ConcurrencyException(
                                $"Document {key} does not exist, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                        // we update the tombstone
                        etag = CreateTombstone(context,
                            lowerKey,
                            lowerSize,
                            keyPtr,
                            keySize,
                            local.Item2.Etag,
                            collectionName,
                            local.Item2.ChangeVector,
                            lastModifiedTicks,
                            changeVector,
                            DocumentFlags.None);

                    }
                }
                if (local.Item1 != null)
                {
                    // just delete the document
                    var doc = local.Item1;
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

                    collectionName = ExtractCollectionName(context, loweredKey, doc.Data);
                    var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                    int size;
                    var ptr = table.DirectRead(doc.StorageId, out size);
                    var tvr = new TableValueReader(ptr, size);

                    lowerKey = tvr.Read((int) DocumentsTable.LoweredKey, out lowerSize);
                    keyPtr = tvr.Read((int) DocumentsTable.Key, out keySize);

                    etag = CreateTombstone(context,
                        lowerKey,
                        lowerSize,
                        keyPtr,
                        keySize,
                        doc.Etag,
                        collectionName,
                        doc.ChangeVector,
                        lastModifiedTicks,
                        changeVector,
                        doc.Flags);

                    if (collectionName.IsSystem == false)
                    {
                        _documentDatabase.BundleLoader.VersioningStorage?.Delete(context, collectionName, loweredKey);
                    }
                    table.Delete(doc.StorageId);

                    DeleteAttachmentsOfDocument(context, loweredKey);
                }
            }

            if (etag == -1)
            {
                return null;
            }

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
                Collection = collectionName,
                Etag = etag
            };

        }

        private static void ThrowInvalidConflictWithTombstone(Slice loweredKey)
        {
            throw new InvalidDataException($"we can't have conflicts and local document/tombstone with the key {loweredKey}");
        }

        private void DeleteAttachmentsOfDocument(DocumentsOperationContext context, Slice loweredDocumentId)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            Slice startSlice;
            using (GetAttachmentPrefix(context, loweredDocumentId.Content.Ptr, loweredDocumentId.Size, out startSlice))
            {
                table.DeleteByPrimaryKeyPrefix(startSlice, before =>
                {
                    Slice hashSlice;
                    using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref before.Reader, out hashSlice))
                    {
                        // we are running just before the delete, so we may still have 1 entry there, the one just
                        // about to be deleted
                        DeleteAttachmentStream(context, hashSlice, expectedCount: 1);
                    }
                });
            }
        }

        public struct DeleteOperationResult
        {
            public long Etag;
            public CollectionName Collection;
        }

        private CollectionName ResolveConflictAndAddTombstone(DocumentsOperationContext context,
            ChangeVectorEntry[] changeVector, IReadOnlyList<DocumentConflict> conflicts, out long etag)
        {
            ChangeVectorEntry[] mergedChangeVector;
            var indexOfLargestEtag = FindIndexOfLargestEtagAndMergeChangeVectors(conflicts, out mergedChangeVector);
            var latestConflict = conflicts[indexOfLargestEtag];
            var collectionName = new CollectionName(latestConflict.Collection);

            byte* lowerKeyPtr;
            byte* keyPtr;
            int lowerKeySize;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, latestConflict.Key,
                out lowerKeyPtr,
                out lowerKeySize,
                out keyPtr,
                out keySize);

            //note that CreateTombstone is also deleting conflicts
            etag = CreateTombstone(context,
                lowerKeyPtr,
                lowerKeySize,
                keyPtr,
                keySize,
                latestConflict.Etag,
                collectionName,
                mergedChangeVector,
                latestConflict.LastModified.Ticks,
                changeVector,
                DocumentFlags.None);

            context.Transaction.AddAfterCommitNotification(new DocumentChange
            {
                Type = DocumentChangeTypes.Delete,
                Etag = etag,
                Key = latestConflict.Key,
                CollectionName = collectionName.Name,
                IsSystemDocument = collectionName.IsSystem,
            });

            return collectionName;
        }

        private static int FindIndexOfLargestEtagAndMergeChangeVectors(IReadOnlyList<DocumentConflict> conflicts, out ChangeVectorEntry[] mergedChangeVectorEntries)
        {
            mergedChangeVectorEntries = null;
            bool firstTime = true;

            int indexOfLargestEtag = 0;
            long largestEtag = 0;
            for (var i = 0; i < conflicts.Count; i++)
            {
                var conflict = conflicts[i];
                if (conflict.Etag > largestEtag)
                {
                    largestEtag = conflict.Etag;
                    indexOfLargestEtag = i;
                }

                if (firstTime)
                {
                    mergedChangeVectorEntries = conflict.ChangeVector;
                    firstTime = false;
                    continue;
                }
                mergedChangeVectorEntries = ReplicationUtils.MergeVectors(mergedChangeVectorEntries, conflict.ChangeVector);
            }

            return indexOfLargestEtag;
        }

        private static void ThrowOnDocumentConflict(DocumentsOperationContext context, Slice loweredKey)
        {
            //TODO: don't forget to refactor this method
            var conflicts = GetConflictsFor(context, loweredKey);
            long largestEtag = 0;
            if (conflicts.Count > 0)
            {
                var conflictRecords = new List<GetConflictsResult.Conflict>();
                foreach (var conflict in conflicts)
                {
                    if (largestEtag < conflict.Etag)
                        largestEtag = conflict.Etag;
                    conflictRecords.Add(new GetConflictsResult.Conflict
                    {
                        ChangeVector = conflict.ChangeVector
                    });
                }

                ThrowDocumentConflictException(loweredKey.ToString(), largestEtag);
            }
        }

        private static void ThrowDocumentConflictException(string docId, long etag)
        {
            throw new DocumentConflictException($"Conflict detected on '{docId}', conflict must be resolved before the document will be accessible.", docId, etag);
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

        private long CreateTombstone(
            DocumentsOperationContext context,
            byte* lowerKey, int lowerSize,
            byte* keyPtr, int keySize,
            long etag,
            CollectionName collectionName,
            ChangeVectorEntry[] docChangeVector,
            long? lastModifiedTicks,
            ChangeVectorEntry[] changeVector,
            DocumentFlags flags)
        {
            var newEtag = GenerateNextEtag();
            var newEtagBigEndian = Bits.SwapBytes(newEtag);
            var documentEtagBigEndian = Bits.SwapBytes(etag);

            Slice loweredKey;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKey))
            {
                if (changeVector == null)
                {
                    changeVector = GetMergedConflictChangeVectorsAndDeleteConflicts(
                        context,
                        loweredKey,
                        newEtag,
                        docChangeVector);
                }
                else
                {
                    DeleteConflictsFor(context, loweredKey);
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
                        (int)flags,
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

        public long GetConflictsMaxEtagFor(DocumentsOperationContext context, Slice loweredKey)
        {
            if (_conflictCount == 0)
                return 0;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            long maxEtag = 0L;
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], loweredKey, 0, startsWith: true))
            {
                var etag = TableValueToEtag((int) ConflictsTable.Etag, ref tvr.Result.Reader);
                if (maxEtag < etag)
                    maxEtag = etag;
            }

            return maxEtag;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, string key)
        {
            if (_conflictCount == 0)
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
            var list = new List<ChangeVectorEntry[]>();
            if (_conflictCount == 0)
                return list;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            while (true)
            {
                var more = false;
                foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], loweredKey, 0, true))
                {
                    more = true;

                    int size;
                    var etag = *(long*)tvr.Result.Reader.Read((int)ConflictsTable.Etag, out size);
                    var cve = tvr.Result.Reader.Read((int)ConflictsTable.ChangeVector, out size);
                    var vector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
                    fixed (ChangeVectorEntry* pVector = vector)
                    {
                        Memory.Copy((byte*)pVector, cve, size);
                    }
                    list.Add(vector);
                    EnsureLastEtagIsPersisted(context, etag);

                    conflictsTable.Delete(tvr.Result.Reader.Id);
                    break;
                }

                if (more == false)
                    break;
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
                    Interlocked.Add(ref _conflictCount, -listCount);
                };
            }
            return list;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, ChangeVectorEntry[] changeVector)
        {
            if (_conflictCount == 0)
                return;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

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
                            Interlocked.Decrement(ref _conflictCount);
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
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            Slice loweredKeySlice;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKeySlice))
            {
                foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], loweredKeySlice, 0, true))
                {
                    int conflictKeySize;
                    var conflictKey = tvr.Result.Reader.Read((int)ConflictsTable.LoweredKey, out conflictKeySize);

                    if (conflictKeySize != lowerSize)
                        break;

                    var compare = Memory.Compare(lowerKey, conflictKey, lowerSize);
                    if (compare != 0)
                        break;

                    var currentChangeVector = GetChangeVectorEntriesFromTableValueReader(ref tvr.Result.Reader, (int)ConflictsTable.ChangeVector);
                    if (currentChangeVector.SequenceEqual(changeVector))
                    {
                        int size;
                        var dataPtr = tvr.Result.Reader.Read((int)ConflictsTable.Data, out size);
                        var doc = (size == 0) ? null : new BlittableJsonReaderObject(dataPtr, size, context);
                        DebugDisposeReaderAfterTransction(context.Transaction, doc);
                        return new DocumentConflict
                        {
                            ChangeVector = currentChangeVector,
                            Key = new LazyStringValue(key, tvr.Result.Reader.Read((int)ConflictsTable.OriginalKey, out size), size, context),
                            StorageId = tvr.Result.Reader.Id,
                            //size == 0 --> this is a tombstone conflict
                            Doc = doc
                        };
                    }
                }
            }
            return null;
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, string key)
        {
            if (_conflictCount == 0)
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
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            var items = new List<DocumentConflict>();
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], loweredKey, 0, true))
            {
                var conflict = TableValueToConflictDocument(context, ref tvr.Result.Reader);
                if (loweredKey.Content.Match(conflict.LoweredKey) == false)
                    break;

                items.Add(conflict);
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
                var mergedChangeVector = ReplicationUtils.MergeVectors(incomingChangeVector, existingDoc.ChangeVector);
                Put(context, key, null, incomingDoc, lastModifiedTicks, mergedChangeVector);
                return true;
            }

            if (existingTombstone != null && incomingDoc == null)
            {
                // Conflict between two tombstones resolves to the local tombstone
                existingTombstone.ChangeVector = ReplicationUtils.MergeVectors(incomingChangeVector, existingTombstone.ChangeVector);
                Slice keySlice;
                using (DocumentKeyWorker.GetSliceFromKey(context, existingTombstone.Key, out keySlice))
                {
                    Delete(context, keySlice, existingTombstone.Key, null,
                    lastModifiedTicks,
                    existingTombstone.ChangeVector,
                    existingTombstone.Collection);
                }
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
                Slice keySlice;
                using (DocumentKeyWorker.GetSliceFromKey(ctx, conflict.LoweredKey, out keySlice))
                {
                    Delete(ctx, keySlice, conflict.LoweredKey, null,
                        _documentDatabase.Time.GetUtcNow().Ticks, conflict.ChangeVector, conflict.Collection);
                    return;
                }
            }

            // because we are resolving to a conflict, and putting a document will
            // delete all the conflicts, we have to create a copy of the document
            // in order to avoid the data we are saving from being removed while
            // we are saving it

            // the resolved document could be an update of the existing document, so it's a good idea to clone it also before updating.
            using (var clone = conflict.Doc.Clone(ctx))
            {
                // handle the case where we resolve a conflict for a document from a different collection
                DeleteDocumentFromDifferentCollectionIfNeeded(ctx, conflict);

                ReplicationUtils.EnsureCollectionTag(clone, conflict.Collection);
                Put(ctx, conflict.LoweredKey, null, clone, null, conflict.ChangeVector);
            }
        }

        private void DeleteDocumentFromDifferentCollectionIfNeeded(DocumentsOperationContext ctx, DocumentConflict conflict)
        {
            Document oldVersion;
            try
            {
                oldVersion = Get(ctx, conflict.LoweredKey);
            }
            catch (DocumentConflictException)
            {
                return; // if already conflicted, don't need to do anything
            }

            if (oldVersion == null)
                return;

            var oldVersionCollectionName = CollectionName.GetCollectionName(oldVersion.Data);
            if (oldVersionCollectionName.Equals(conflict.Collection, StringComparison.OrdinalIgnoreCase))
                return;

            DeleteWithoutCreatingTombstone(ctx, oldVersionCollectionName, oldVersion.StorageId, isTombstone: false);
        }

        private bool ValidatedResolveByScriptInput(ScriptResolver scriptResolver,
            IReadOnlyList<DocumentConflict> conflicts,
            LazyStringValue collection)
        {
            if (scriptResolver == null)
                return false;
            if (collection == null)
                return false;
            if (conflicts.Count < 2)
                return false;

            foreach (var documentConflict in conflicts)
            {
                if (collection != documentConflict.Collection)
                {
                    var msg = $"All conflicted documents must have same collection name, but we found conflicted document in {collection} and an other one in {documentConflict.Collection}";
                    if (_logger.IsInfoEnabled)
                        _logger.Info(msg);

                    var differentCollectionNameAlert = AlertRaised.Create(
                        $"Script unable to resolve conflicted documents with the key {documentConflict.Key}",
                        msg,
                        AlertType.Replication,
                        NotificationSeverity.Error,
                        "Mismatched Collections On Replication Resolve"
                        );
                    _documentDatabase.NotificationCenter.Add(differentCollectionNameAlert);
                    return false;
                }
            }

            return true;
        }

        public bool TryResolveConflictByScriptInternal(
            DocumentsOperationContext context,
            ScriptResolver scriptResolver,
            IReadOnlyList<DocumentConflict> conflicts,
            LazyStringValue collection,
            bool hasLocalTombstone)
        {

            if (ValidatedResolveByScriptInput(scriptResolver, conflicts, collection) == false)
            {
                return false;
            }

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
            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);

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
                    Interlocked.Increment(ref _conflictCount);
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
                    Interlocked.Increment(ref _conflictCount);
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
                LazyStringValue lazyCollectionName;
                if (incomingDoc != null) // can be null if it is a tombstone
                {
                    doc = incomingDoc.BasePointer;
                    docSize = incomingDoc.Size;
                    lazyCollectionName = CollectionName.GetLazyCollectionNameFrom(context, incomingDoc);
                }
                else
                {
                    lazyCollectionName = context.GetLazyString(incomingTombstoneCollection);
                }

                using (lazyCollectionName)
                {
                    var tvb = new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*incomingChangeVector.Length},
                        {keyPtr, keySize},
                        {doc, docSize},
                        Bits.SwapBytes(GenerateNextEtag()),
                        {lazyCollectionName.Buffer, lazyCollectionName.Size},
                        docPositions.LastModifiedTicks
                    };

                    Interlocked.Increment(ref _conflictCount);
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
            ChangeVectorEntry[] changeVector = null,
            DocumentFlags flags = DocumentFlags.None)
        {
            if (context.Transaction == null)
            {
                ThrowRequiresTransaction();
                return default(PutOperationResults);// never reached
            }
#if DEBUG
            var documentDebugHash = document.DebugHash;
            document.BlittableValidation();
#endif

            BlittableJsonReaderObject.AssertNoModifications(document, key, assertChildren: true);

            var collectionName = ExtractCollectionName(context, key, document);
            var newEtag = GenerateNextEtag();
            var newEtagBigEndian = Bits.SwapBytes(newEtag);

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

            if (_conflictCount != 0)
            {
                // Since this document resolve the conflict we dont need to alter the change vector.
                // This way we avoid another replication back to the source
                if (expectedEtag.HasValue)
                {
                    Slice keySlice;
                    long currentMaxConflictEtag;
                    using (Slice.External(context.Allocator, lowerKey, lowerSize, out keySlice))
                    {
                        currentMaxConflictEtag = GetConflictsMaxEtagFor(context, keySlice);
                    }

                    ThrowConcurrencyExceptionOnConflict(expectedEtag, currentMaxConflictEtag);
                }

                var fromReplication = (flags & DocumentFlags.FromReplication) == DocumentFlags.FromReplication;
                if (fromReplication)
                {
                    DeleteConflictsFor(context, key);
                }
                else
                {
                    changeVector = MergeConflictChangeVectorIfNeededAndDeleteConflicts(changeVector, context, key, newEtag);
                }
            }


            // delete a tombstone if it exists, if it known that it is a new key, no need, so we can skip it
            if (knownNewKey == false)
            {
                DeleteTombstoneIfNeeded(context, collectionName, lowerKey, lowerSize);
            }

            var modifiedTicks = lastModifiedTicks ?? _documentDatabase.Time.GetUtcNow().Ticks;

            Slice loweredKey;
            using (Slice.External(context.Allocator, lowerKey, (ushort)lowerSize, out loweredKey))
            {
                var oldValue = default(TableValueReader);
                if (knownNewKey == false)
                {
                    table.ReadByKey(loweredKey, out oldValue);
                }

                if (changeVector == null)
                {
                    var oldChangeVector = oldValue.Pointer != null ? GetChangeVectorEntriesFromTableValueReader(ref oldValue, 4) : null;
                    changeVector = SetDocumentChangeVectorForLocalChange(context,
                        loweredKey,
                        oldChangeVector, newEtag);
                }

                if (collectionName.IsSystem == false && (flags & DocumentFlags.Artificial) != DocumentFlags.Artificial)
                {
                    if (_documentDatabase.BundleLoader.VersioningStorage != null)
                    {
                        VersioningConfigurationCollection configuration;
                        var version = _documentDatabase.BundleLoader.VersioningStorage.ShouldVersionDocument(collectionName, document, out configuration);
                        if (version)
                        {
                            flags |= DocumentFlags.Versioned;
                            _documentDatabase.BundleLoader.VersioningStorage.PutFromDocument(context, key, document, changeVector, configuration);
                        }
                    }
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
                        (int)flags,
                        transactionMarker
                    };

                    if (oldValue.Pointer == null)
                    {
                        if (expectedEtag != null && expectedEtag != 0)
                        {
                            ThrowConcurrentExceptionOnMissingDoc(key, expectedEtag.Value);
                        }
                        table.Insert(tbv);
                    }
                    else
                    {
                        var oldEtag = TableValueToEtag(1, ref oldValue);
                        //TODO
                        if (expectedEtag != null && oldEtag != expectedEtag)
                            ThrowConcurrentException(key, expectedEtag, oldEtag);

                        int oldSize;
                        var oldDoc = new BlittableJsonReaderObject(oldValue.Read((int) DocumentsTable.Data, out oldSize), oldSize, context);
                        var oldCollectionName = ExtractCollectionName(context, key, oldDoc);
                        if (oldCollectionName != collectionName)
                            ThrowInvalidCollectionNameChange(key, oldCollectionName, collectionName);

                        table.Update(oldValue.Id, tbv);
                    }
                }

                if (collectionName.IsSystem == false)
                {
                    _documentDatabase.BundleLoader.ExpiredDocumentsCleaner?.Put(context,
                        loweredKey, document);
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

#if DEBUG
            if (document.DebugHash != documentDebugHash)
            {
                throw new InvalidDataException("The incoming document " + key + " has changed _during_ the put process, this is likely because you are trying to save a document that is already stored and was moved");
            }
#endif

            return new PutOperationResults
            {
                Etag = newEtag,
                Key = key,
                Collection = collectionName
            };
        }

        private static void ThrowConcurrencyExceptionOnConflict(long? expectedEtag, long currentMaxConflictEtag)
        {
            throw new ConcurrencyException(
                $"Tried to resolve document conflict with etag = {expectedEtag}, but the current max conflict etag is {currentMaxConflictEtag}. This means that the conflict information with which you are trying to resolve the conflict is outdated. Get conflict information and try resolving again.");
        }

        private static void ThrowConcurrentExceptionOnMissingDoc(string key, long expectedEtag)
        {
            throw new ConcurrencyException(
                $"Document {key} does not exist, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedETag = expectedEtag
            };
        }

        private static void ThrowInvalidCollectionNameChange(string key, CollectionName oldCollectionName,
            CollectionName collectionName)
        {
            throw new InvalidOperationException(
                $"Changing '{key}' from '{oldCollectionName.Name}' to '{collectionName.Name}' via update is not supported.{System.Environment.NewLine}" +
                $"Delete it and recreate the document {key}.");
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
                    mergedChangeVectorEntries = ReplicationUtils.MergeVectors(mergedChangeVectorEntries, documentChangeVector);

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

        private static void ThrowRequiresTransaction([CallerMemberName]string caller = null)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentException("Context must be set with a valid transaction before calling " + caller, "context");
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
            ChangeVectorEntry[] oldChangeVector, long newEtag)
        {
            if (oldChangeVector != null)
                return ReplicationUtils.UpdateChangeVectorWithNewEtag(Environment.DbId, newEtag, oldChangeVector);

            return GetMergedConflictChangeVectorsAndDeleteConflicts(context, loweredKey, newEtag);
        }

        private ChangeVectorEntry[] GetMergedConflictChangeVectorsAndDeleteConflicts(
            DocumentsOperationContext context,
            Slice loweredKey,
            long newEtag,
            ChangeVectorEntry[] existing = null)
        {
            if (_conflictCount == 0)
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
            var identities = context.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
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
            var identities = context.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
            var nextIdentityValue = identities.Increment(key, 1);
            var finalKey = AppendIdentityValueToKey(key, nextIdentityValue);
            Slice finalKeySlice;
            tries = 1;

            using (DocumentKeyWorker.GetSliceFromKey(context, finalKey, out finalKeySlice))
            {
                TableValueReader reader;
                if (table.ReadByKey(finalKeySlice, out reader) == false)
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
                    if (table.ReadByKey(finalKeySlice, out reader) == false)
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
            var identities = ctx.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
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

        public void DeleteTombstonesBefore(string collection, long etag, Transaction transaction)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return;

            var table = transaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones));
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
            var readTree = context.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
            foreach (var identity in identities)
            {
                readTree.AddMax(identity.Key, identity.Value);
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

        private CollectionName GetCollection(string collection, bool throwIfDoesNotExist)
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

        public AttachmentResult PutAttachment(
            DocumentsOperationContext context,
            string documentId,
            string name,
            string contentType,
            string hash,
            long? expectedEtag,
            Stream stream,
            long? lastModifiedTicks = null)
        {
            if (context.Transaction == null)
            {
                ThrowRequiresTransaction();
                return default(AttachmentResult); // never reached
            }

            // Attachment etag should be generated before updating the document
            var attachmenEtag = GenerateNextEtag();
            var modifiedTicks = lastModifiedTicks ?? _documentDatabase.Time.GetUtcNow().Ticks;

            Slice lowerDocumentId;
            DocumentKeyWorker.GetSliceFromKey(context, documentId, out lowerDocumentId);

            TableValueReader tvr;
            var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out tvr);
            if (hasDoc == false)
                throw new InvalidOperationException($"Cannot put attachment {name} on a non existent document '{documentId}'.");

            // Update the document with an etag which is bigger than the attachmenEtag
            var putResult = UpdateDocumentForAttachmentChange(context, documentId, tvr, modifiedTicks);

            byte* lowerName;
            int lowerNameSize;
            byte* namePtr;
            int nameSize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, name, out lowerName, out lowerNameSize, out namePtr, out nameSize);

            Slice keySlice, contentTypeSlice, hashSlice;
            using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName, lowerNameSize, out keySlice))
            using (DocumentKeyWorker.GetStringPreserveCase(context, contentType, out contentTypeSlice))
            using (Slice.From(context.Allocator, hash, out hashSlice)) // Hash is a base64 string, so this is a special case that we do not need to escape
            {
                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                var tbv = new TableValueBuilder
                {
                    {keySlice.Content.Ptr, keySlice.Size},
                    Bits.SwapBytes(attachmenEtag),
                    {namePtr, nameSize},
                    {contentTypeSlice.Content.Ptr, contentTypeSlice.Size},
                    {hashSlice.Content.Ptr, hashSlice.Size},
                    modifiedTicks,
                };

                TableValueReader oldValue;
                if (table.ReadByKey(keySlice, out oldValue))
                {
                    // TODO: Support overwrite
                    throw new NotImplementedException("Cannot overwrite an exisitng attachment.");

                    /*
                    var oldEtag = TableValueToEtag(context, 1, ref oldValue);
                    if (expectedEtag != null && oldEtag != expectedEtag)
                        throw new ConcurrencyException($"Attachment {name} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                        {
                            ActualETag = oldEtag,
                            ExpectedETag = expectedEtag ?? -1
                        };

                    table.Update(oldValue.Id, tbv);*/
                }
                else
                {
                    if (expectedEtag.HasValue && expectedEtag.Value != 0)
                    {
                        ThrowConcurrentExceptionOnMissingAttacment(documentId, name, expectedEtag.Value);
                    }

                    table.Insert(tbv);
                }

                PutAttachmentStream(context, keySlice, hashSlice, stream);

                _documentDatabase.Metrics.AttachmentPutsPerSecond.MarkSingleThreaded(1);
                _documentDatabase.Metrics.AttachmentBytesPutsPerSecond.MarkSingleThreaded(stream.Length);

                context.Transaction.AddAfterCommitNotification(new AttachmentChange
                {
                    Etag = attachmenEtag,
                    CollectionName = putResult.Collection.Name,
                    Key = documentId,
                    Name = name,
                    Type = DocumentChangeTypes.PutAttachment,
                    IsSystemDocument = putResult.Collection.IsSystem,
                });
            }

            return new AttachmentResult
            {
                Etag = attachmenEtag,
                ContentType = contentType,
                Name = name,
                DocumentId = documentId,
                Hash = hash,
            };
        }

        private void PutAttachmentStream(DocumentsOperationContext context, Slice key, Slice hash, Stream stream)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            var existingStream = tree.ReadStream(hash);
            if (existingStream == null)
                tree.AddStream(hash, stream, tag: key);
        }

        private void DeleteAttachmentStream(DocumentsOperationContext context, Slice hash, int expectedCount = 0)
        {
            if (GetCountOfAttachmentsForHash(context, hash) == expectedCount)
            {
                var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
                tree.DeleteStream(hash);
            }
        }

        private bool TryGetDocumentTableValueReaderForAttachment(DocumentsOperationContext context, string documentId,
            string name, Slice loweredKey, out TableValueReader tvr)
        {
            bool hasDoc;
            try
            {
                hasDoc = GetTableValueReaderForDocument(context, loweredKey, out tvr);
            }
            catch (DocumentConflictException e)
            {
                throw new InvalidOperationException($"Cannot put/delete an attachment {name} on a document '{documentId}' when it has an unresolved conflict.", e);
            }
            return hasDoc;
        }

        private PutOperationResults UpdateDocumentForAttachmentChange(
            DocumentsOperationContext context, 
            string documentId, 
            TableValueReader tvr, 
            long modifiedTicks)
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
                return Put(context, documentId, null, data, modifiedTicks, null, DocumentFlags.HasAttachments);
            }
            finally
            {
                context.ReturnMemory(copyOfDoc);
            }
        }

        private IEnumerable<Attachment> GetAttachmentsForDocument(DocumentsOperationContext context, Slice startSlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            foreach (var sr in table.SeekByPrimaryKeyStartingWith(startSlice, Slices.Empty, 0))
            {
                var attachment = TableValueToAttachment(context, ref sr.Reader);
                if (attachment == null)
                    continue;
                yield return attachment;
            }
        }

        public long GetNumberOfAttachments(DocumentsOperationContext context)
        {
            // We count in also versioned streams
            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            return tree.State.NumberOfEntries;
        }

        [Conditional("DEBUG")]
        public void AssertNoAttachmentsForDocument(DocumentsOperationContext context, string documentId)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            byte* lowerDocumentId;
            int lowerDocumentIdSize;
            byte* documentIdPtr; // not in use
            int documentIdSize; // not in use
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, documentId, out lowerDocumentId, out lowerDocumentIdSize,
                out documentIdPtr, out documentIdSize);

            Slice startSlice;
            using (GetAttachmentPrefix(context, lowerDocumentId, lowerDocumentIdSize, out startSlice))
            {
                foreach (var sr in table.SeekByPrimaryKeyStartingWith(startSlice, Slices.Empty, 0))
                {
                    var attachment = TableValueToAttachment(context, ref sr.Reader);
                    throw new InvalidOperationException($"Found attachment {attachment.Name} but it should be deleted.");
                }
            }
        }

        private bool IsAttachmentDeleted(ref TableValueReader reader)
        {
            int size;
            reader.Read((int)AttachmentsTable.Name, out size);
            return size == 0;
        }

        public Attachment GetAttachment(DocumentsOperationContext context, string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            Slice lowerKey, lowerName, keySlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, documentId, out lowerKey))
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out lowerName))
            using (GetAttachmentKey(context, lowerKey.Content.Ptr, lowerKey.Size, lowerName.Content.Ptr, lowerName.Size, out keySlice))
            {
                var attachment = GetAttachment(context, keySlice);
                if (attachment == null)
                    return null;

                var stream = GetAttachmentStream(context, attachment.Base64Hash);
                if (stream == null)
                    throw new FileNotFoundException($"Attachment's stream {name} on {documentId} was not found. This should not happen and is likely a bug.");
                attachment.Stream = stream;

                return attachment;
            }
        }

        public Attachment GetAttachment(DocumentsOperationContext context, Slice keySlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            TableValueReader tvr;
            if (table.ReadByKey(keySlice, out tvr) == false)
                return null;

            return TableValueToAttachment(context, ref tvr);
        }

        private Stream GetAttachmentStream(DocumentsOperationContext context, Slice hashSlice)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            return tree.ReadStream(hashSlice);
        }

        private ReleaseMemory GetAttachmentKey(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize, byte* lowerName, int lowerNameSize, out Slice keySlice)
        {
            var keyMem = context.Allocator.Allocate(lowerKeySize + 1 + lowerNameSize);
            Memory.CopyInline(keyMem.Ptr, lowerKey, lowerKeySize);
            keyMem.Ptr[lowerKeySize] = (byte)30; // the record separator
            Memory.CopyInline(keyMem.Ptr + lowerKeySize + 1, lowerName, lowerNameSize);
            keySlice = new Slice(SliceOptions.Key, keyMem);
            return new ReleaseMemory(keyMem, context);
        }

        private ReleaseMemory GetAttachmentPrefix(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize, out Slice startSlice)
        {
            var keyMem = context.Allocator.Allocate(lowerKeySize + 1);
            Memory.CopyInline(keyMem.Ptr, lowerKey, lowerKeySize);
            keyMem.Ptr[lowerKeySize] = (byte)30; // the record separator
            startSlice = new Slice(SliceOptions.Key, keyMem);
            return new ReleaseMemory(keyMem, context);
        }

        private Attachment TableValueToAttachment(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var isDeleted = IsAttachmentDeleted(ref tvr);
            if (isDeleted)
                return null;

            var result = new Attachment
            {
                StorageId = tvr.Id
            };

            result.LoweredKey = TableValueToString(context, (int)AttachmentsTable.LoweredDocumentIdAndRecordSeparatorAndLoweredName, ref tvr);
            result.Etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);
            result.Name = TableValueToKey(context, (int)AttachmentsTable.Name, ref tvr);
            result.ContentType = TableValueToKey(context, (int)AttachmentsTable.ContentType, ref tvr);

            int size;
            result.LastModified = new DateTime(*(long*)tvr.Read((int)AttachmentsTable.LastModified, out size));

            TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            return result;
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
            return new LazyStringValue(null, ptr, size, context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LazyStringValue TableValueToKey(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            byte offset;
            var ptr = tvr.Read(index, out size);
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            return new LazyStringValue(null, ptr + offset, size, context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope TableValueToSlice(
            DocumentsOperationContext context, int index, ref TableValueReader tvr, out Slice slice)
        {
            int size;
            var ptr = tvr.Read(index, out size);
            return Slice.External(context.Allocator, ptr, size, out slice);
        }

        private static void ThrowConcurrentExceptionOnMissingAttacment(string documentId, string name, long expectedEtag)
        {
            throw new ConcurrencyException(
                $"Attachment {name} of '{documentId}' does not exist, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedETag = expectedEtag
            };
        }

        public void DeleteAttachment(DocumentsOperationContext context, string documentId, string name, long? expectedEtag)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            Slice lowerDocumentId, lowerName;
            using (DocumentKeyWorker.GetSliceFromKey(context, documentId, out lowerDocumentId))
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out lowerName))
            {
                Slice keySlice;
                using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size, out keySlice))
                {
                    DeleteAttachment(context, keySlice, lowerDocumentId, documentId, name, expectedEtag);
                }
            }
        }

        private void DeleteAttachment(DocumentsOperationContext context, Slice keySlice, Slice lowerDocumentId, string documentId, string name, long? expectedEtag)
        {
            var attachmenEtag = GenerateNextEtag();
            var modifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;

            TableValueReader docTvr;
            var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out docTvr);
            if (hasDoc == false)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException(
                        $"Document {documentId} does not exist, but delete was called with etag {expectedEtag} to remove attachment {name}. Optimistic concurrency violation, transaction will be aborted.");

                // this basically mean that we tried to delete attachment whose document doesn't exist.
                return;
            }
            var putResult = UpdateDocumentForAttachmentChange(context, documentId, docTvr, modifiedTicks);

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            TableValueReader tvr;
            if (table.ReadByKey(keySlice, out tvr) == false)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException($"Attachment {name} of document {documentId} does not exist, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                // this basically mean that we tried to delete attachment that doesn't exist.
                return;
            }

            var etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);
            if (expectedEtag != null && etag != expectedEtag)
            {
                throw new ConcurrencyException($"Attachment {name} of document '{documentId}' has etag {etag}, but Delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                {
                    ActualETag = etag,
                    ExpectedETag = (long)expectedEtag
                };
            }

            Slice hashSlice;
            using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out hashSlice))
            {
                var tbv = new TableValueBuilder
                {
                    {keySlice.Content.Ptr, keySlice.Size},
                    Bits.SwapBytes(attachmenEtag),
                    {null, 0},
                    {null, 0},
                    {null, 0},
                    modifiedTicks,
                };
                table.Update(tvr.Id, tbv);

                DeleteAttachmentStream(context, hashSlice);
            }

            context.Transaction.AddAfterCommitNotification(new AttachmentChange
            {
                Etag = attachmenEtag,
                CollectionName = putResult.Collection.Name,
                Key = documentId,
                Name = name,
                Type = DocumentChangeTypes.Delete,
                IsSystemDocument = putResult.Collection.IsSystem,
            });
        }
    }
}