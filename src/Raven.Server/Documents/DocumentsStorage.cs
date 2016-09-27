using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Replication.Messages;
using Raven.Server.Exceptions;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;

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

        private static readonly TableSchema DocsSchema = new TableSchema();
        private static readonly Slice TombstonesPrefix;
        private static readonly Slice DeletedEtagsSlice;
        private static readonly TableSchema ConflictsSchema = new TableSchema();
        private static readonly TableSchema TombstonesSchema = new TableSchema();
        private static readonly TableSchema CollectionsSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;

        private Dictionary<string, CollectionName> _collectionsCache;

        static DocumentsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "AllTombstonesEtags", ByteStringType.Immutable, out AllTombstonesEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastEtag", ByteStringType.Immutable, out LastEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Key", ByteStringType.Immutable, out KeySlice);
            Slice.From(StorageEnvironment.LabelsContext, "Docs", ByteStringType.Immutable, out DocsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Tombstones", ByteStringType.Immutable, out TombstonesSlice);
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndChangeVector", ByteStringType.Immutable, out KeyAndChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, CollectionName.GetTablePrefix(CollectionTableType.Tombstones), ByteStringType.Immutable, out TombstonesPrefix);
            Slice.From(StorageEnvironment.LabelsContext, "DeletedEtags", ByteStringType.Immutable, out DeletedEtagsSlice);
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
                StartIndex = 1,
                Count = 1,
                IsGlobal = false,
                Name = KeySlice
            });

            // required to get conflicts by key
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 2,
                IsGlobal = false,
                Name = KeyAndChangeVectorSlice
            });

            // The documents schema is as follows
            // 5 fields (lowered key, etag, lazy string key, document, change vector)
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

        public string DataDirectory;
        public DocumentsContextPool ContextPool;
        private UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

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
                _logger.Info
                    ("Starting to open document storage for " + (_documentDatabase.Configuration.Core.RunInMemory ?
                    "<memory>" : _documentDatabase.Configuration.Core.DataDirectory));

            var options = _documentDatabase.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(_documentDatabase.Configuration.Core.DataDirectory);

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
                    tx.CreateTree("Docs");
                    tx.CreateTree("LastReplicatedEtags");
                    tx.CreateTree("Identities");
                    tx.CreateTree("ChangeVector");
                    ConflictsSchema.Create(tx, "Conflicts");
                    CollectionsSchema.Create(tx, "Collections");

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
            var changeVector = new ChangeVectorEntry[tree.State.NumberOfEntries];
            using (var iter = tree.Iterate(false))
            {
                if (iter.Seek(Slices.BeforeAllKeys) == false)
                    return changeVector;
                var buffer = new byte[sizeof(Guid)];
                int index = 0;
                do
                {
                    var read = iter.CurrentKey.CreateReader().Read(buffer, 0, sizeof(Guid));
                    if (read != sizeof(Guid))
                        throw new InvalidDataException($"Expected guid, but got {read} bytes back for change vector");

                    changeVector[index].DbId = new Guid(buffer);
                    changeVector[index].Etag = iter.CreateReaderForCurrent().ReadBigEndianInt64();
                    index++;
                } while (iter.MoveNext());
            }
            return changeVector;
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, Dictionary<Guid, long> changeVector)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree("ChangeVector");
            Guid dbId;
            long etagBigEndian;
            Slice keySlice; ;
            Slice valSlice;
            using (Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid), out keySlice))
            using (Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long), out valSlice))
            {
                foreach (var kvp in changeVector)
                {
                    dbId = kvp.Key;
                    etagBigEndian = IPAddress.HostToNetworkOrder(kvp.Value);
                    tree.Add(keySlice, valSlice);
                }
            }
        }

        public static long ReadLastDocumentEtag(Transaction tx)
        {
            var fst = new FixedSizeTree(tx.LowLevelTransaction,
                tx.LowLevelTransaction.RootObjects,
                AllDocsEtagsSlice, sizeof(long));

            using (var it = fst.Iterate())
            {
                if (it.SeekToLast())
                    return it.CurrentKey;
            }

            return 0;
        }

        public static long ReadLastTombstoneEtag(Transaction tx)
        {
            var fst = new FixedSizeTree(tx.LowLevelTransaction,
                tx.LowLevelTransaction.RootObjects,
                AllTombstonesEtagsSlice, sizeof(long));

            using (var it = fst.Iterate())
            {
                if (it.SeekToLast())
                    return it.CurrentKey;
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

            return lastEtag;
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string prefix, string matches, string exclude, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            Slice prefixSlice;
            using (GetSliceFromKey(context, prefix, out prefixSlice))
            {
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var result in table.SeekByPrimaryKey(prefixSlice, startsWith: true))
                {
                    var document = TableValueToDocument(context, result);
                    string documentKey = document.Key;
                    if (documentKey.StartsWith(prefix) == false)
                        break;

                    if (!WildcardMatcher.Matches(matches, documentKey) ||
                        WildcardMatcher.MatchesExclusion(exclude, documentKey))
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
                yield return TableValueToDocument(context, result);
            }
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, string collection, int start, int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

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
                yield return TableValueToDocument(context, result);
            }
        }

        public IEnumerable<Document> GetDocumentsAfter(DocumentsOperationContext context, long etag, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag))
            {
                if (result.Id == etag)
                    continue;

                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                {
                    yield break;
                }

                yield return TableValueToDocument(context, result);
            }
        }

        public IEnumerable<Document> GetDocumentsAfter(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag))
            {
                if (result.Id == etag)
                    continue;

                yield return TableValueToDocument(context, result);
            }
        }

        public IEnumerable<Document> GetDocuments(DocumentsOperationContext context, List<Slice> ids, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            foreach (var id in ids)
            {
                // id must be lowercased

                var tvr = table.ReadByKey(id);
                if (tvr == null)
                    continue;

                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;

                yield return TableValueToDocument(context, tvr);
            }
        }

        public IEnumerable<Document> GetDocumentsAfter(DocumentsOperationContext context, string collection, long etag, int start, int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], etag))
            {
                if (result.Id == etag)
                    continue;

                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, result);
            }
        }

        public Tuple<Document, DocumentTombstone> GetDocumentOrTombstone(DocumentsOperationContext context, string key, bool throwOnConflict = true)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            Slice loweredKey;
            using (GetSliceFromKey(context, key, out loweredKey))
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
            var tvr = tombstoneTable.ReadByKey(loweredKey);

            return Tuple.Create<Document, DocumentTombstone>(null, TableValueToTombstone(context, tvr));
        }

        public Document Get(DocumentsOperationContext context, string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            Slice loweredKey;
            using (GetSliceFromKey(context, key, out loweredKey))
            {
                return Get(context, loweredKey);
            }
        }

        public Document Get(DocumentsOperationContext context, Slice loweredKey)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            var tvr = table.ReadByKey(loweredKey);
            if (tvr == null)
            {
                ThrowDocumentConflictIfNeeded(context, loweredKey);
                return null;
            }

            var doc = TableValueToDocument(context, tvr);

            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(doc);

            return doc;
        }

        public IEnumerable<DocumentTombstone> GetTombstonesAfter(
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

                yield return TableValueToTombstone(context, result);
            }
        }

        public IEnumerable<DocumentTombstone> GetTombstonesAfter(
            DocumentsOperationContext context,
            string collection,
            long etag,
            int start,
            int take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));

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

                yield return TableValueToTombstone(context, result);
            }
        }

        public long GetLastDocumentEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

            var result = table
                        .SeekBackwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue)
                        .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Read(1, out size);
            return IPAddress.NetworkToHostOrder(*(long*)ptr);
        }

        public long GetLastTombstoneEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));

            var result = table
                .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], long.MaxValue)
                .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Read(1, out size);
            return Bits.SwapBytes(*(long*)ptr);
        }

        public long GetNumberOfTombstonesWithDocumentEtagLowerThan(DocumentsOperationContext context, string collection, long etag)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));

            return table
                    .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[DeletedEtagsSlice], etag)
                    .Count();
        }

        public static ByteStringContext.Scope GetSliceFromKey(DocumentsOperationContext context, string key, out Slice keySlice)
        {
            var byteCount = Encoding.UTF8.GetMaxByteCount(key.Length);

            var buffer = context.GetNativeTempBuffer(
                byteCount
                + sizeof(char) * key.Length); // for the lower calls

            fixed (char* pChars = key)
            {
                var destChars = (char*)buffer;
                for (var i = 0; i < key.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }

                var keyBytes = buffer + key.Length * sizeof(char);

                var size = Encoding.UTF8.GetBytes(destChars, key.Length, keyBytes, byteCount);

                if (size > 512)
                    ThrowKeyTooBig(key, size);

                return Slice.External(context.Allocator, keyBytes, (ushort)size, out keySlice);
            }
        }

        public static void GetLowerKeySliceAndStorageKey(JsonOperationContext context, string str, out byte* lowerKey, out int lowerSize,
            out byte* key, out int keySize)
        {
            // Because we need to also store escape positions for the key when we store it
            // we need to store it as a lazy string value.
            // But lazy string value has two lengths, one is the string length, and the other 
            // is the actual data size with the escape positions

            // In order to resolve this, we process the key to find escape positions, then store it 
            // in the table using the following format:
            //
            // [var int - string len, string bytes, number of escape positions, escape positions]
            //
            // The total length of the string is stored in the actual table (and include the var int size 
            // prefix.

            var byteCount = Encoding.UTF8.GetMaxByteCount(str.Length);
            var jsonParserState = new JsonParserState();
            jsonParserState.FindEscapePositionsIn(str);
            var maxKeyLenSize = JsonParserState.VariableSizeIntSize(byteCount);
            var escapePositionsSize = jsonParserState.GetEscapePositionsSize();
            var buffer = context.GetNativeTempBuffer(
                sizeof(char) * str.Length // for the lower calls
                + byteCount // lower key
                + maxKeyLenSize // the size of var int for the len of the key
                + byteCount // actual key
                + escapePositionsSize);

            fixed (char* pChars = str)
            {
                var destChars = (char*)buffer;
                for (var i = 0; i < str.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }

                lowerKey = buffer + str.Length * sizeof(char);

                lowerSize = Encoding.UTF8.GetBytes(destChars, str.Length, lowerKey, byteCount);

                if (lowerSize > 512)
                    ThrowKeyTooBig(str, lowerSize);

                key = buffer + str.Length * sizeof(char) + byteCount;
                var writePos = key;
                keySize = Encoding.UTF8.GetBytes(pChars, str.Length, writePos + maxKeyLenSize, byteCount);

                var actualKeyLenSize = JsonParserState.VariableSizeIntSize(keySize);
                if (actualKeyLenSize < maxKeyLenSize)
                {
                    var movePtr = maxKeyLenSize - actualKeyLenSize;
                    key += movePtr;
                    writePos += movePtr;
                }

                JsonParserState.WriteVariableSizeInt(ref writePos, keySize);
                jsonParserState.WriteEscapePositionsTo(writePos + keySize);
                keySize += escapePositionsSize + maxKeyLenSize;
            }
        }

        private static void ThrowKeyTooBig(string str, int lowerSize)
        {
            throw new ArgumentException(
                $"Key cannot exceed 512 bytes, but the key was {lowerSize} bytes. The invalid key is '{str}'.",
                nameof(str));
        }

        public static Document TableValueToDocument(JsonOperationContext context, TableValueReader tvr)
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

            return result;
        }

        private static DocumentConflict TableValueToConflictDocument(JsonOperationContext context, TableValueReader tvr)
        {
            var result = new DocumentConflict
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
            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 1);
            result.Doc = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);

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

        private static DocumentTombstone TableValueToTombstone(JsonOperationContext context, TableValueReader tvr)
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
            result.Etag = IPAddress.NetworkToHostOrder(*(long*)ptr);
            ptr = tvr.Read(2, out size);
            result.DeletedEtag = Bits.SwapBytes(*(long*)ptr);

            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 4);

            result.Collection = new LazyStringValue(null, tvr.Read(5, out size), size, context);

            return result;
        }

        public bool Delete(DocumentsOperationContext context, string key, long? expectedEtag)
        {
            Slice keySlice;
            using (GetSliceFromKey(context, key, out keySlice))
            {
                return Delete(context, keySlice, expectedEtag);
            }
        }

        public bool Delete(DocumentsOperationContext context,
            Slice loweredKey,
            long? expectedEtag,
            ChangeVectorEntry[] changeVector = null)
        {
            var result = GetDocumentOrTombstone(context, loweredKey);
            if (result.Item2 != null)
                return false; //NOP, already deleted

            var doc = result.Item1;
            if (doc == null)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException(
                        $"Document {loweredKey} does not exists, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                ThrowDocumentConflictIfNeeded(context, loweredKey);
                return false;
            }

            if (expectedEtag != null && doc.Etag != expectedEtag)
            {
                throw new ConcurrencyException(
                    $"Document {loweredKey} has etag {doc.Etag}, but Delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
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

            CreateTombstone(context,
                lowerKey,
                lowerSize,
                keyPtr,
                keySize,
                doc.Etag,
                collectionName,
                doc.ChangeVector,
                changeVector);

            if (collectionName.IsSystem == false)
            {
                _documentDatabase.BundleLoader.VersioningStorage?.Delete(context, collectionName, loweredKey);
            }
            table.Delete(doc.StorageId);

            context.Transaction.AddAfterCommitNotification(new DocumentChangeNotification
            {
                Type = DocumentChangeTypes.Delete,
                Etag = expectedEtag,
                MaterializeKey = state => ((Slice)state).ToString(),
                MaterializeKeyState = loweredKey,
                CollectionName = collectionName.Name,
                IsSystemDocument = collectionName.IsSystem,
            });

            return true;
        }

        private void ThrowDocumentConflictIfNeeded(DocumentsOperationContext context, string key)
        {
            var conflicts = GetConflictsFor(context, key);
            if (conflicts.Count > 0)
                throw new DocumentConflictException(key, conflicts);
        }

        private void ThrowDocumentConflictIfNeeded(DocumentsOperationContext context, Slice loweredKey)
        {
            var conflicts = GetConflictsFor(context, loweredKey);
            if (conflicts.Count > 0)
                throw new DocumentConflictException(loweredKey.ToString(), conflicts);
        }

        private void EnsureLastEtagIsPersisted(DocumentsOperationContext context, long docEtag)
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
            ChangeVectorEntry[] changeVector,
            string collection)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: true);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);
            Slice loweredKey;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKey))
            {
                ThrowDocumentConflictIfNeeded(context, loweredKey);

                var result = GetDocumentOrTombstone(context, loweredKey);
                if (result.Item2 != null) //already have a tombstone -> need to update the change vector
                {
                    UpdateTombstoneChangeVector(context, changeVector, result.Item2, lowerKey, lowerSize, keyPtr,
                        keySize);
                }
                else
                {
                    var doc = result.Item1;
                    var newEtag = ++_lastEtag;

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

                    context.Transaction.AddAfterCommitNotification(new DocumentChangeNotification
                    {
                        Type = DocumentChangeTypes.DeleteOnTombstoneReplication,
                        Etag = _lastEtag,
                        MaterializeKey = state => ((Slice)state).ToString(),
                        MaterializeKeyState = loweredKey,
                        CollectionName = collectionName.Name,
                        IsSystemDocument = false, //tombstone is not a system document...
                    });
                }
            }
        }

        private void UpdateTombstoneChangeVector(
            DocumentsOperationContext context,
            ChangeVectorEntry[] changeVector,
            DocumentTombstone tombstone,
            byte* lowerKey, int lowerSize,
            byte* keyPtr, int keySize)
        {
            var collectionName = GetCollection(tombstone.Collection, throwIfDoesNotExist: true);

            tombstone.ChangeVector = changeVector;
            var tombstoneTables = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            var newEtag = ++_lastEtag;
            var newEtagBigEndian = Bits.SwapBytes(newEtag);
            var documentEtag = tombstone.DeletedEtag;
            var documentEtagBigEndian = Bits.SwapBytes(documentEtag);

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                //update change vector and etag of the tombstone, other values are unchanged
                var tbv = new TableValueBuilder
                {
                    {lowerKey, lowerSize},
                    {(byte*) &newEtagBigEndian, sizeof(long)},
                    {(byte*) &documentEtagBigEndian, sizeof(long)},
                    {keyPtr, keySize},
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                    {tombstone.Collection.Buffer, tombstone.Collection.Size}
                };
                tombstoneTables.Set(tbv);
            }
        }

        private void CreateTombstone(
            DocumentsOperationContext context,
            byte* lowerKey, int lowerSize,
            byte* keyPtr, int keySize,
            long etag,
            CollectionName collectionName,
            ChangeVectorEntry[] docChangeVector,
            ChangeVectorEntry[] changeVector)
        {
            var newEtag = ++_lastEtag;
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
                    var tbv = new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) &newEtagBigEndian, sizeof(long)},
                        {(byte*) &documentEtagBigEndian, sizeof(long)},
                        {keyPtr, keySize},
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                        collectionSlice
                    };

                    var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                        collectionName.GetTableName(CollectionTableType.Tombstones));

                    table.Insert(tbv);
                }
            }
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, string key)
        {

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            Slice keySlice;
            using (Slice.External(context.Allocator, lowerKey, keySize, out keySlice))
            {
                DeleteConflictsFor(context, keySlice);
            }
        }


        public IReadOnlyList<ChangeVectorEntry[]> DeleteConflictsFor(DocumentsOperationContext context, Slice loweredKey)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            var list = new List<ChangeVectorEntry[]>();
            while (true)
            {
                bool deleted = false;
                // deleting a value might cause other ids to change, so we can't just pass the list
                // of ids to be deleted, because they wouldn't remain stable during the deletions
                foreach (var tvr in conflictsTable.SeekByPrimaryKey(loweredKey, startsWith: true))
                {
                    deleted = true;

                    int size;
                    var cve = tvr.Read(1, out size);
                    var vector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
                    fixed (ChangeVectorEntry* pVector = vector)
                    {
                        Memory.Copy((byte*)pVector, cve, size);
                    }
                    list.Add(vector);

                    conflictsTable.Delete(tvr.Id);
                    break;
                }
                if (deleted == false)
                    return list;
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
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            Slice loweredKeySlice;
            using (Slice.External(context.Allocator, lowerKey, lowerSize, out loweredKeySlice))
            {
                foreach (var result in conflictsTable.SeekForwardFrom(
                    ConflictsSchema.Indexes[KeyAndChangeVectorSlice],
                    loweredKeySlice, true))
                {
                    foreach (var tvr in result.Results)
                    {

                        int conflictKeySize;
                        var conflictKey = tvr.Read(0, out conflictKeySize);

                        if (conflictKeySize != lowerSize)
                            break;

                        var compare = Memory.Compare(lowerKey, conflictKey, lowerSize);
                        if (compare != 0)
                            break;

                        var currentChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 1);
                        if (currentChangeVector.Equals(changeVector))
                        {
                            int size;
                            return new DocumentConflict
                            {
                                ChangeVector = currentChangeVector,
                                Key = new LazyStringValue(key, tvr.Read(2, out size), size, context),
                                StorageId = tvr.Id,
                                Doc = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context)
                            };
                        }
                    }
                }
            }
            return null;
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, string key)
        {
            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);
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
                    var conflictKey = tvr.Read(0, out conflictKeySize);

                    if (conflictKeySize != loweredKey.Size)
                        break;

                    var compare = Memory.Compare(loweredKey.Content.Ptr, conflictKey, loweredKey.Size);
                    if (compare != 0)
                        break;

                    items.Add(TableValueToConflictDocument(context, tvr));
                }
            }

            return items;
        }

        public void AddConflict(DocumentsOperationContext context, string key, BlittableJsonReaderObject incomingDoc,
            ChangeVectorEntry[] incomingChangeVector)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {key} (Incoming change vector {incomingChangeVector.Format()})");
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            // ReSharper disable once ArgumentsStyleLiteral
            var existing = GetDocumentOrTombstone(context, key, throwOnConflict: false);
            if (existing.Item1 != null)
            {
                var existingDoc = existing.Item1;
                fixed (ChangeVectorEntry* pChangeVector = existingDoc.ChangeVector)
                {
                    conflictsTable.Set(new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, existingDoc.ChangeVector.Length*sizeof(ChangeVectorEntry)},
                        {keyPtr, keySize},
                        {existingDoc.Data.BasePointer, existingDoc.Data.Size}
                    });

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    EnsureLastEtagIsPersisted(context, existingDoc.Etag);
                    var collectionName = ExtractCollectionName(context, existingDoc.Key, existingDoc.Data);

                    //make sure that the relevant collection tree exists
                    var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                    table.Delete(existingDoc.StorageId);
                }
            }
            if (existing.Item2 != null)
            {
                var existingTombstone = existing.Item2;
                fixed (ChangeVectorEntry* pChangeVector = existingTombstone.ChangeVector)
                {
                    conflictsTable.Set(new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, existingTombstone .ChangeVector.Length*sizeof(ChangeVectorEntry)},
                        {keyPtr, keySize},
                        {null,0}
                    });

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    EnsureLastEtagIsPersisted(context, existingTombstone.Etag);

                    var collectionName = GetCollection(existingTombstone.Collection, throwIfDoesNotExist: true);

                    var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
                    table.Delete(existingTombstone.StorageId);
                }
            }

            fixed (ChangeVectorEntry* pChangeVector = incomingChangeVector)
            {
                byte* doc = null;
                int docSize = 0;
                if (incomingDoc != null) // can be null if it is a tombstone
                {
                    doc = incomingDoc.BasePointer;
                    docSize = incomingDoc.Size;
                }

                var tvb = new TableValueBuilder
                {
                    {lowerKey, lowerSize},
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*incomingChangeVector.Length},
                    {keyPtr, keySize},
                    {doc, docSize}
                };


                conflictsTable.Set(tvb);
            }
        }

        public PutResult Put(DocumentsOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document,
            ChangeVectorEntry[] changeVector = null)
        {
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put",
                    nameof(context));

            var collectionName = ExtractCollectionName(context, key, document);
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

            if (string.IsNullOrWhiteSpace(key))
                key = Guid.NewGuid().ToString();

            if (key[key.Length - 1] == '/')
            {
                key = GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, table, context);
            }

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            ThrowDocumentConflictIfNeeded(context, key);

            // delete a tombstone if it exists
            DeleteTombstoneIfNeeded(context, collectionName, lowerKey, lowerSize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = Bits.SwapBytes(newEtag);

            TableValueReader oldValue;
            Slice keySlice;
            using (Slice.External(context.Allocator, lowerKey, (ushort)lowerSize, out keySlice))
            {
                oldValue = table.ReadByKey(keySlice);

                if (changeVector == null)
                {
                    changeVector = SetDocumentChangeVectorForLocalChange(context,
                        keySlice,
                        oldValue, newEtag);
                }

                fixed (ChangeVectorEntry* pChangeVector = changeVector)
                {
                    var tbv = new TableValueBuilder
                    {
                        {lowerKey, lowerSize}, //0
                        {(byte*) &newEtagBigEndian, sizeof(long)}, //1
                        {keyPtr, keySize}, //2
                        {document.BasePointer, document.Size}, //3
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length} //4
                    };

                    if (oldValue == null)
                    {
                        if (expectedEtag != null && expectedEtag != 0)
                        {
                            throw new ConcurrencyException(
                                $"Document {key} does not exists, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
                        }
                        table.Insert(tbv);
                    }
                    else
                    {
                        int size;
                        var pOldEtag = oldValue.Read(1, out size);
                        var oldEtag = IPAddress.NetworkToHostOrder(*(long*)pOldEtag);
                        if (expectedEtag != null && oldEtag != expectedEtag)
                            throw new ConcurrencyException(
                                $"Document {key} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                        int oldSize;
                        var oldDoc = new BlittableJsonReaderObject(oldValue.Read(3, out oldSize), oldSize, context);
                        var oldCollectionName = ExtractCollectionName(context, key, oldDoc);
                        if (oldCollectionName != collectionName)
                            throw new InvalidOperationException(
                                $"Changing '{key}' from '{oldCollectionName.Name}' to '{collectionName.Name}' via update is not supported.{System.Environment.NewLine}" +
                                $"Delete the document and recreate the document {key}.");

                        table.Update(oldValue.Id, tbv);
                    }
                }

                if (collectionName.IsSystem == false)
                {
                    _documentDatabase.BundleLoader.VersioningStorage?.PutFromDocument(context, collectionName, key,
                        newEtagBigEndian, document);
                    _documentDatabase.BundleLoader.ExpiredDocumentsCleaner?.Put(context,
                        keySlice, document);
                }
            }

            context.Transaction.AddAfterCommitNotification(new DocumentChangeNotification
            {
                Etag = newEtag,
                CollectionName = collectionName.Name,
                Key = key,
                Type = DocumentChangeTypes.Put,
                IsSystemDocument = collectionName.IsSystem,
            });

            return new PutResult
            {
                ETag = newEtag,
                Key = key
            };
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
                return UpdateChangeVectorWithLocalChange(newEtag, changeVector);
            }

            return GetMergedConflictChangeVectorsAndDeleteConflicts(context, loweredKey, newEtag);
        }

        private ChangeVectorEntry[] GetMergedConflictChangeVectorsAndDeleteConflicts(
            DocumentsOperationContext context,
            Slice loweredKey,
            long newEtag,
            ChangeVectorEntry[] existing = null)
        {
            var conflictChangeVectors = DeleteConflictsFor(context, loweredKey);
            if (conflictChangeVectors.Count == 0)
            {
                if (existing != null)
                    return UpdateChangeVectorWithLocalChange(newEtag, existing);

                return new[]
                {
                    new ChangeVectorEntry
                    {
                        Etag = newEtag,
                        DbId = Environment.DbId
                    }
                };
            }

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

        private ChangeVectorEntry[] UpdateChangeVectorWithLocalChange(long newEtag, ChangeVectorEntry[] changeVector)
        {
            var length = changeVector.Length;
            for (int i = 0; i < length; i++)
            {
                if (changeVector[i].DbId == Environment.DbId)
                {
                    changeVector[i].Etag = newEtag;
                    return changeVector;
                }
            }
            Array.Resize(ref changeVector, length + 1);
            changeVector[length].DbId = Environment.DbId;
            changeVector[length].Etag = newEtag;
            return changeVector;
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

        private string GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key, Table table, DocumentsOperationContext context)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree("Identities");
            var nextIdentityValue = identities.Increment(key, 1);

            var finalKey = key + nextIdentityValue;
            Slice finalKeySlice;
            using (GetSliceFromKey(context, finalKey, out finalKeySlice))
            {
                if (table.ReadByKey(finalKeySlice) == null)
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
                finalKey = key + maybeFree;
                using (GetSliceFromKey(context, finalKey, out finalKeySlice))
                {
                    if (table.ReadByKey(finalKeySlice) == null)
                    {
                        if (lastKnownBusy + 1 == maybeFree)
                        {
                            nextIdentityValue = identities.Increment(key, maybeFree);
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

        public long IdentityFor(DocumentsOperationContext ctx, string key)
        {
            var identities = ctx.Transaction.InnerTransaction.ReadTree("Identities");
            return identities.Increment(key, 1);
        }

        public long GetNumberOfDocuments(DocumentsOperationContext context)
        {
            var fstIndex = DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public class CollectionStat
        {
            public string Name;
            public long Count;
        }

        public IEnumerable<CollectionStat> GetCollections(DocumentsOperationContext context)
        {
            foreach (var kvp in _collectionsCache)
            {
                var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema, kvp.Value.GetTableName(CollectionTableType.Documents));

                yield return new CollectionStat
                {
                    Name = kvp.Key,
                    Count = collectionTable.NumberOfEntries
                };
            }
        }

        public CollectionStat GetCollection(string collection, DocumentsOperationContext context)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                return new CollectionStat
                {
                    Name = collection,
                    Count = 0
                };
            }

            var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

            return new CollectionStat
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

            var table = transaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleting tombstones earlier than {etag} in {collection}");
            table.DeleteBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, long.MaxValue);
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
                throw new InvalidOperationException($"There is not collection for '{collection}'.");

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

                DocsSchema.Create(context.Transaction.InnerTransaction, name.GetTableName(CollectionTableType.Documents));
                TombstonesSchema.Create(context.Transaction.InnerTransaction,
                    name.GetTableName(CollectionTableType.Tombstones));

                // safe to do, other transactions will see it, but we are under write lock here
                _collectionsCache = new Dictionary<string, CollectionName>(_collectionsCache,
                    StringComparer.OrdinalIgnoreCase)
                {
                    {name.Name, name}
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
                    var ptr = tvr.Read(0, out size);
                    var collection = new LazyStringValue(null, ptr, size, context);

                    result.Add(collection, new CollectionName(collection));
                }
            }

            return result;
        }
    }
}