using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Replication.Messages;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;
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
        private readonly DocumentDatabase _documentDatabase;

        private static readonly TableSchema DocsSchema = new TableSchema();
        private static readonly TableSchema ConflictsSchema = new TableSchema();
        private static readonly TableSchema TombstonesSchema = new TableSchema();

        static DocumentsStorage()
        {
            /*
             The structure of conflicts table starts with the following fields:
             [ Conflicted Doc Id | Change Vector | ... the rest of fields ... ]
             PK of the conflicts table will be 'Change Vector' field, because when dealing with conflicts,
              the change vectors will always be different, hence the uniqueness of the key. (inserts/updates will not overwrite)

            Additional indice is set to have composite key of 'Conflicted Doc Id' and 'Change Vector' so we will be able to iterate
            on conflicts by conflicted doc id (using 'starts with')
             */
            ConflictsSchema.DefineKey(
                new TableSchema.SchemaIndexDef
                {
                    StartIndex = 1,
                    Count = 1,
                    IsGlobal = false,
                    Name = "Key"
                });

            //not sure that this is needed, probably needs to be deleted
            ConflictsSchema.DefineIndex("KeyAndChangeVector",
                new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                    Count = 2,
                    IsGlobal = false,
                    Name = "KeyAndChangeVector"
                });

            // The documents schema is as follows
            // 4 fields (lowered key, etag, lazy string key, document, change vector)
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            DocsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = "Docs"
            });

            DocsSchema.DefineFixedSizeIndex("CollectionEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false
            });

            DocsSchema.DefineFixedSizeIndex("AllDocsEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true
            });

            TombstonesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = "Tombstones"
            });

            TombstonesSchema.DefineFixedSizeIndex("CollectionEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false
            });

            TombstonesSchema.DefineFixedSizeIndex("AllTombstonesEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true
            });

            TombstonesSchema.DefineFixedSizeIndex("DeletedEtags", new TableSchema.FixedSizeSchemaIndexDef()
            {
                StartIndex = 2,
                IsGlobal = false
            });
        }

        private readonly Logger _logger;
        private readonly string _name;

        private static readonly Slice AllDocsEtagsSlice = Slice.From(StorageEnvironment.LabelsContext, "AllDocsEtags", ByteStringType.Immutable);
        private static readonly Slice LastEtagSlice = Slice.From(StorageEnvironment.LabelsContext, "LastEtag", ByteStringType.Immutable);
        private static readonly Slice HashTagSlice = Slice.From(StorageEnvironment.LabelsContext, "#", ByteStringType.Immutable);

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
                    ("Starting to open document storage for " +  (_documentDatabase.Configuration.Core.RunInMemory ?
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
                    DocsSchema.Create(tx, Document.SystemDocumentsCollection);
                    ConflictsSchema.Create(tx, "Conflicts");
                    _lastEtag = ReadLastEtag(tx);

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
            foreach (var kvp in changeVector)
            {
                var dbId = kvp.Key;
                var etagBigEndian = IPAddress.HostToNetworkOrder(kvp.Value);
                tree.Add(Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid)),
                   Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long)));
            }
        }

        public static long ReadLastEtag(Transaction tx)
        {
            var tree = tx.CreateTree("Etags");
            var readResult = tree.Read(LastEtagSlice);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            var fst = new FixedSizeTree(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects, AllDocsEtagsSlice, sizeof(long));

            using (var it = fst.Iterate())
            {
                if (it.SeekToLast())
                {
                    lastEtag = Math.Max(lastEtag, it.CurrentKey);
                }
            }
            return lastEtag;
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string prefix, string matches, string exclude, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            var prefixSlice = GetSliceFromKey(context, prefix);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekByPrimaryKey(prefixSlice, startsWith:true))
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

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, int start, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(DocsSchema.FixedSizeIndexes["AllDocsEtags"], long.MaxValue))
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
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, "@" + collection);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(DocsSchema.FixedSizeIndexes["CollectionEtags"], long.MaxValue))
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
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes["AllDocsEtags"], etag))
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

        public IEnumerable<Document> GetDocumentsAfter(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes["AllDocsEtags"], etag))
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
            var collectionName = "@" + collection;
            if (context.Transaction.InnerTransaction.ReadTree(collectionName) == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes["CollectionEtags"], etag))
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

        public Tuple<Document, DocumentTombstone> GetDocumentOrTombstone(DocumentsOperationContext context, string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            var loweredKey = GetSliceFromKey(context, key);
            return GetDocumentOrTombstone(context,loweredKey);
        }

        public Tuple<Document, DocumentTombstone> GetDocumentOrTombstone(DocumentsOperationContext context, Slice loweredKey)
        {
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            var doc = Get(context, loweredKey);
            if (doc != null)
                return Tuple.Create<Document, DocumentTombstone>(doc, null);

            var tombstoneTable = new Table(TombstonesSchema, context.Transaction.InnerTransaction);
            var tvr = tombstoneTable.ReadByKey(loweredKey);
            return Tuple.Create<Document, DocumentTombstone>(null, TableValueToTombstone(context, tvr));
        }

        public Document Get(DocumentsOperationContext context, string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            var loweredKey = GetSliceFromKey(context, key);

            return Get(context, loweredKey);
        }

        public Document Get(DocumentsOperationContext context, Slice loweredKey)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            var tvr = table.ReadByKey(loweredKey);
            if (tvr == null)
                return null;

            var doc = TableValueToDocument(context, tvr);

            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(doc.Key, doc.Data.Size);

            return doc;
        }

        public IEnumerable<DocumentTombstone> GetTombstonesAfter(DocumentsOperationContext context, string collection, long etag, int start, int take)
        {
            Table table;
            try
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, "#" + collection);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                yield break;
            }

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes["CollectionEtags"], etag))
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
            Table table;
            try
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, "@" + collection);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return 0;
            }

            var result = table
                        .SeekBackwardFrom(DocsSchema.FixedSizeIndexes["CollectionEtags"], long.MaxValue)
                        .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Read(1, out size);
            return IPAddress.NetworkToHostOrder(*(long*)ptr);
        }

        public long GetLastTombstoneEtag(DocumentsOperationContext context, string collection)
        {
            Table table;
            try
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, "#" + collection);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return 0;
            }

            var result = table
                .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes["CollectionEtags"], long.MaxValue)
                .FirstOrDefault();

            if (result == null)
                return 0;

            int size;
            var ptr = result.Read(1, out size);
            return IPAddress.NetworkToHostOrder(*(long*)ptr);
        }

        public long GetNumberOfTombstonesWithDocumentEtagLowerThan(DocumentsOperationContext context, string collection, long etag)
        {
            Table table;
            try
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, "#" + collection);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return 0;
            }
            return table
                    .SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes["DeletedEtags"], etag)
                    .Count();
        }

        private Slice GetSliceFromKey(DocumentsOperationContext context, string key)
        {
            // TODO: Can we do better here?

            var byteCount = Encoding.UTF8.GetMaxByteCount(key.Length);
            if (byteCount > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {byteCount} bytes. The invalid key is '{key}'.",
                    nameof(key));

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
                return Slice.External(context.Allocator, keyBytes, (ushort)size);
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

        private static Document TableValueToDocument(JsonOperationContext context, TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id
            };
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            var ptr = tvr.Read(2, out size);
            byte offset;
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);
            ptr = tvr.Read(1, out size);
            result.Etag = Bits.SwapBytes(*(long*)ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);

            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 4);

            return result;
        }


        private static ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(TableValueReader tvr, int index)
        {
            int size;
            var pChangeVector = (ChangeVectorEntry*) tvr.Read(index, out size);
            var changeVector = new ChangeVectorEntry[size/sizeof (ChangeVectorEntry)];
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
            var ptr = tvr.Read(3, out size);
            byte offset;
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);

            ptr = tvr.Read(1, out size);
            result.Etag = IPAddress.NetworkToHostOrder(*(long*)ptr);
            ptr = tvr.Read(2, out size);
            result.DeletedEtag = IPAddress.NetworkToHostOrder(*(long*)ptr);

            result.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 3);

            result.Collection = new LazyStringValue(null, tvr.Read(4, out size), size, context);

            return result;
        }

        public bool Delete(DocumentsOperationContext context, string key, long? expectedEtag)
        {
            return Delete(context, GetSliceFromKey(context, key), expectedEtag);
        }

        public bool Delete(DocumentsOperationContext context, Slice loweredKey, long? expectedEtag, ChangeVectorEntry[] changeVector = null)
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

                return false;
            }

            if (expectedEtag != null && doc.Etag != expectedEtag)
            {
                throw new ConcurrencyException(
                    $"Document {loweredKey} has etag {doc.Etag}, but Delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
            }

            EnsureLastEtagIsPersisted(context, doc.Etag);

            string originalCollectionName;
            bool isSystemDocument;
            var collectionName = GetCollectionName(loweredKey, doc.Data, out originalCollectionName, out isSystemDocument);
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName);

            CreateTombstone(context, table, doc, originalCollectionName, changeVector);

            if (isSystemDocument == false)
            {
                _documentDatabase.BundleLoader.VersioningStorage?.Delete(context, originalCollectionName, loweredKey);
            }
            table.Delete(doc.StorageId);

            context.Transaction.AddAfterCommitNotification(new DocumentChangeNotification
            {
                Type = DocumentChangeTypes.Delete,
                Etag = expectedEtag,
                MaterializeKey = state => ((Slice)state).ToString(),
                MaterializeKeyState = loweredKey,
                CollectionName = originalCollectionName,
                IsSystemDocument = isSystemDocument,
            });

            return true;
        }

        private void EnsureLastEtagIsPersisted(DocumentsOperationContext context, long docEtag)
        {
            if (docEtag != _lastEtag)
                return;
            var etagTree = context.Transaction.InnerTransaction.ReadTree("Etags");
            var etag = _lastEtag;
            etagTree.Add(LastEtagSlice, Slice.External(context.Allocator, (byte*) &etag, sizeof(long)));
        }

        private void CreateTombstone(DocumentsOperationContext context, 
            Table collectionDocsTable, 
            Document doc, 
            string collectionName, 
            ChangeVectorEntry[] changeVector)
        {
            int size;
            var ptr = collectionDocsTable.DirectRead(doc.StorageId, out size);
            var tvr = new TableValueReader(ptr, size);

            int lowerSize;
            var lowerKey = tvr.Read(0, out lowerSize);

            int keySize;
            var keyPtr = tvr.Read(2, out keySize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = Bits.SwapBytes(newEtag);
            var documentEtagBigEndian = Bits.SwapBytes(doc.Etag);

            if (changeVector == null)
            {
                changeVector = GetMergedConflictChangeVectorsAndDeleteConflicts(
                        context,
                        Slice.External(context.Allocator, lowerKey, lowerSize), 
                        newEtag,
                        doc.ChangeVector);
            }

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                var collectionSlice = Slice.From(context.Allocator, collectionName);
                var tbv = new TableValueBuilder
                {
                    {lowerKey, lowerSize},
                    {(byte*) &newEtagBigEndian, sizeof (long)},
                    {(byte*) &documentEtagBigEndian, sizeof (long)},
                    {keyPtr, keySize},
                    {(byte*)pChangeVector, sizeof (ChangeVectorEntry)*changeVector.Length},
                    {collectionSlice}
                };

                var col = "#" + collectionName; // TODO: We need a way to turn a string to a prefixed value that doesn't involve allocations
                TombstonesSchema.Create(context.Transaction.InnerTransaction, col);
                var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, col);

                table.Insert(tbv);
            }
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, string key)
        {

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var keySlice = Slice.External(context.Allocator, lowerKey, keySize);
            DeleteConflictsFor(context, keySlice);

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
                    var vector = new ChangeVectorEntry[size/sizeof(ChangeVectorEntry)];
                    fixed (ChangeVectorEntry* pVector = vector)
                    {
                        Memory.Copy((byte*) pVector, cve, size);
                    }
                    list.Add(vector);

                    conflictsTable.Delete(tvr.Id);
                    break;
                }
                if (deleted == false)
                    return list;
            }
        }
        
        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, string key)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var items = new List<DocumentConflict>();			
            foreach (var result in conflictsTable.SeekForwardFrom(
                ConflictsSchema.Indexes["KeyAndChangeVector"],
                Slice.External(context.Allocator, lowerKey, lowerSize),true))
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

                    int size;
                    items.Add(new DocumentConflict
                    {
                        ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, 1),
                        Key = new LazyStringValue(key, tvr.Read(2, out size), size, context),
                        StorageId = tvr.Id,
                        Doc = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context)
                    });
                }
            }

            return items;
        }

        public void AddConflict(DocumentsOperationContext context, string key, BlittableJsonReaderObject incomingDoc,
            ChangeVectorEntry[] incomingChangeVector)
        {
            if(_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {key} (Incoming change vector {string.Join(",", incomingChangeVector.Select(x => $"{x.DbId}/{x.Etag}"))})");
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, "Conflicts");

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var existing = GetDocumentOrTombstone(context, key);
            if (existing.Item1 != null)
            {
                var existingDoc = existing.Item1;
                fixed (ChangeVectorEntry* pChangeVector = existingDoc.ChangeVector)
                {
                    conflictsTable.Set(new TableValueBuilder
                    {
                        {lowerKey, lowerSize},
                        {(byte*) pChangeVector, existingDoc.ChangeVector.Length*sizeof(ChangeVectorEntry)},
                        {existingDoc.Key.Buffer, existingDoc.Key.Size},
                        {existingDoc.Data.BasePointer, existingDoc.Data.Size}
                    });

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    EnsureLastEtagIsPersisted(context, existingDoc.Etag);
                    bool isSystemDocument;
                    var collectionName = Document.GetCollectionName(existingDoc.Data, out isSystemDocument);

                    //make sure that the relevant collection tree exists
                    var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, "@"+collectionName);

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
                        {existingTombstone .Key.Buffer, existingTombstone .Key.Size},
                    });

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    EnsureLastEtagIsPersisted(context, existingTombstone.Etag);
                    var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, existingTombstone.Collection);
                    table.Delete(existingTombstone.StorageId);
                }
            }

            fixed (ChangeVectorEntry* pChangeVector = incomingChangeVector)
            {
                conflictsTable.Set(new TableValueBuilder
                {
                    {lowerKey, lowerSize},
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*incomingChangeVector.Length},
                    {keyPtr, keySize},
                    {incomingDoc.BasePointer, incomingDoc.Size}
                });
            }
        }

        public PutResult Put(DocumentsOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document,
            ChangeVectorEntry[] changeVector = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Document key cannot be null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put",
                    nameof(context));

            string originalCollectionName;
            bool isSystemDocument;
            var collectionName = GetCollectionName(key, document, out originalCollectionName, out isSystemDocument);
            DocsSchema.Create(context.Transaction.InnerTransaction, collectionName);
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName);

            if (key[key.Length - 1] == '/')
            {
                key = GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, table, context);
            }

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var col = "#" + originalCollectionName;// TODO: We need a way to turn a string to a prefixed value that doesn't involve allocations
            TombstonesSchema.Create(context.Transaction.InnerTransaction, col);
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, col);
            // delete a tombstone if it exists
            tombstoneTable.DeleteByKey(Slice.From(context.Allocator, lowerKey, lowerSize));

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = Bits.SwapBytes(newEtag);

            var oldValue = table.ReadByKey(Slice.External(context.Allocator, lowerKey, (ushort) lowerSize));

            if (changeVector == null)
            {
                changeVector = SetDocumentChangeVectorForLocalChange(context, 
                    Slice.External(context.Allocator, lowerKey, lowerSize),
                    oldValue, newEtag);
            }

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                var tbv = new TableValueBuilder
                {
                    {lowerKey, lowerSize}, //0
                    {(byte*) &newEtagBigEndian, sizeof (long)}, //1
                    {keyPtr, keySize}, //2
                    {document.BasePointer, document.Size}, //3
                    {(byte*) pChangeVector, sizeof (ChangeVectorEntry)*changeVector.Length} //4
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
                    var oldEtag = IPAddress.NetworkToHostOrder(*(long*) pOldEtag);
                    if (expectedEtag != null && oldEtag != expectedEtag)
                        throw new ConcurrencyException(
                            $"Document {key} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");

                    int oldSize;
                    var oldDoc = new BlittableJsonReaderObject(oldValue.Read(3, out oldSize), oldSize, context);
                    var oldCollectionName = Document.GetCollectionName(key, oldDoc, out isSystemDocument);
                    if (oldCollectionName != originalCollectionName)
                        throw new InvalidOperationException(
                            $"Changing '{key}' from '{oldCollectionName}' to '{originalCollectionName}' via update is not supported.{System.Environment.NewLine}" +
                            $"Delete the document and recreate the document {key}.");

                    table.Update(oldValue.Id, tbv);
                }
            }

            if (isSystemDocument == false)
            {
                _documentDatabase.BundleLoader.VersioningStorage?.PutFromDocument(context, originalCollectionName, key,
                    newEtagBigEndian, document);
                _documentDatabase.BundleLoader.ExpiredDocumentsCleaner?.Put(context,
                    Slice.External(context.Allocator, lowerKey, (ushort) lowerSize), document);
            }

            context.Transaction.AddAfterCommitNotification(new DocumentChangeNotification
            {
                Etag = newEtag,
                CollectionName = originalCollectionName,
                Key = key,
                Type = DocumentChangeTypes.Put,
                IsSystemDocument = isSystemDocument,
            });

            return new PutResult
            {
                ETag = newEtag,
                Key = key
            };
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
            if (table.ReadByKey(GetSliceFromKey(context, finalKey)) == null)
            {
                return finalKey;
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
                if (table.ReadByKey(GetSliceFromKey(context, finalKey)) == null)
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

        private static string GetCollectionName(string key, BlittableJsonReaderObject document, out string originalCollectionName, out bool isSystemDocument)
        {
            var collectionName = Document.GetCollectionName(key, document, out isSystemDocument);

            originalCollectionName = collectionName;

            // TODO: we have to have some way to distinguish between dynamic tree names
            // and our fixed ones, otherwise a collection call Docs will corrupt our state
            return "@" + collectionName;
        }

        private static string GetCollectionName(Slice key, BlittableJsonReaderObject document, out string originalCollectionName, out bool isSystemDocument)
        {
            var collectionName = Document.GetCollectionName(key, document, out isSystemDocument);

            originalCollectionName = collectionName;

            // TODO: we have to have some way to distinguish between dynamic tree names
            // and our fixed ones, otherwise a collection call Docs will corrupt our state
            return "@" + collectionName;
        }

        public long IdentityFor(DocumentsOperationContext ctx, string key)
        {
            var identities = ctx.Transaction.InnerTransaction.ReadTree("Identities");
            return identities.Increment(key, 1);
        }

        public long GetNumberOfDocuments(DocumentsOperationContext context)
        {
            var fstIndex = DocsSchema.FixedSizeIndexes["AllDocsEtags"];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.NameAsSlice, sizeof(long));
            return fst.NumberOfEntries;
        }

        public class CollectionStat
        {
            public string Name;
            public long Count;
        }

        public IEnumerable<CollectionStat> GetCollections(DocumentsOperationContext context)
        {
            using (var it = context.Transaction.InnerTransaction.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    if (context.Transaction.InnerTransaction.GetRootObjectType(it.CurrentKey) != RootObjectType.VariableSizeTree)
                        continue;

                    if (it.CurrentKey[0] != '@') // collection prefix
                        continue;

                    var collectionTableName = it.CurrentKey.ToString();

                    yield return GetCollection(collectionTableName, context);
                } while (it.MoveNext());
            }
        }

        public CollectionStat GetCollection(string collectionName, DocumentsOperationContext context)
        {
            if (collectionName[0] != '@')
                collectionName = "@" + collectionName;

            try
            {
                var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName);

                return new CollectionStat
                {
                    Name = collectionName.Substring(1),
                    Count = collectionTable.NumberOfEntries
                };
            }
            catch (InvalidDataException)
            {
                return new CollectionStat
                {
                    Name = collectionName.Substring(1),
                    Count = 0
                };
            }
        }

        public void DeleteTombstonesBefore(string collection, long etag, Transaction transaction)
        {
            Table table;
            try
            {
                table = transaction.OpenTable(TombstonesSchema, "#" + collection);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return;
            }
            if (_logger.IsInfoEnabled)
                _logger.Info($"Deleting tombstones earlier than {etag} in {collection}");
            table.DeleteBackwardFrom(TombstonesSchema.FixedSizeIndexes["CollectionEtags"], etag, long.MaxValue);
        }

        public IEnumerable<string> GetTombstoneCollections(Transaction transaction)
        {
            using (var it = transaction.LowLevelTransaction.RootObjects.Iterate(false))
            {
                it.RequiredPrefix = HashTagSlice;

                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    var tombstoneCollection = it.CurrentKey.ToString();
                    yield return tombstoneCollection.Substring(1); // removing '#'
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
            etagsTree.Add(
                Slice.From(context.Allocator, dbId),
                Slice.External(context.Allocator, (byte*) &etag, sizeof (long))
                );
        }

    }
}