using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Replication;
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

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private readonly DocumentDatabase _documentDatabase;

        private readonly TableSchema _docsSchema = new TableSchema();
        private readonly TableSchema _tombstonesSchema = new TableSchema();

        private readonly ILog _log;
        private readonly string _name;

        private static readonly Slice AllDocsEtagsSlice = Slice.From(StorageEnvironment.LabelsContext, "AllDocsEtags", ByteStringType.Immutable);
        private static readonly Slice LastEtagSlice = Slice.From(StorageEnvironment.LabelsContext, "LastEtag", ByteStringType.Immutable);
        private static readonly Slice HashTagSlice = Slice.From(StorageEnvironment.LabelsContext, "#", ByteStringType.Immutable);

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        public string DataDirectory;
        public DocumentsContextPool ContextPool;
        private UnmanagedBuffersPool _unmanagedBuffersPool;

        public DocumentsStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _name = _documentDatabase.Name;
            _log = LogManager.GetLogger(typeof(DocumentsStorage).FullName + "." + _name);

            // The documents schema is as follows
            // 4 fields (lowered key, etag, lazy string key, document)
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            _docsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = "Docs"
            });
            _docsSchema.DefineFixedSizeIndex("CollectionEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false
            });
            _docsSchema.DefineFixedSizeIndex("AllDocsEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true
            });

            _tombstonesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = "Tombstones"
            });
            _tombstonesSchema.DefineFixedSizeIndex("CollectionEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false
            });
            _tombstonesSchema.DefineFixedSizeIndex("AllTombstonesEtags", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = true
            });
            _tombstonesSchema.DefineFixedSizeIndex("DeletedEtags", new TableSchema.FixedSizeSchemaIndexDef()
            {
                StartIndex = 2,
                IsGlobal = false
            });
        }

        public StorageEnvironment Environment { get; private set; }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_log, $"Could not dispose {nameof(DocumentsStorage)}");

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
            if (_log.IsDebugEnabled)
            {
                _log.Debug("Starting to open document storage for {0}", _documentDatabase.Configuration.Core.RunInMemory ? "<memory>" : _documentDatabase.Configuration.Core.DataDirectory);
            }
            var options = _documentDatabase.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(_documentDatabase.Configuration.Core.DataDirectory);

            options.IoMetrics = new IoMetrics(32, 10, 12); // ADIADI :: what are the numbers ? should despose (sp, interlocked things) ?

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
                Environment = new StorageEnvironment(options, _documentDatabase.LoggerSetup);
                _unmanagedBuffersPool = new UnmanagedBuffersPool(_name);
                ContextPool = new DocumentsContextPool(_unmanagedBuffersPool, _documentDatabase);

                using (var tx = Environment.WriteTransaction())
                {
                    tx.CreateTree("Docs");
                    tx.CreateTree("Identities");
                    tx.CreateTree("Tombstones");
                    tx.CreateTree("ChangeVector");
                    _docsSchema.Create(tx, Document.SystemDocumentsCollection);
                    _lastEtag = ReadLastEtag(tx);

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_log.IsWarnEnabled)
                {
                    _log.FatalException("Could not open server store for " + _name, e);
                }
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

            var tree = context.Transaction.InnerTransaction.CreateTree("ChangeVector");
            var changeVector = new ChangeVectorEntry[tree.State.NumberOfEntries];
            using (var iter = tree.Iterate(false))
            {
                if (iter.Seek(Slices.BeforeAllKeys) == false)
                    return changeVector;
                const int GuidSizeInBytes = 16;
                var buffer = new byte[GuidSizeInBytes];
                int index = 0;
                do
                {
                    var read = iter.CurrentKey.CreateReader().Read(buffer, 0, GuidSizeInBytes);
                    if (read != GuidSizeInBytes)
                        throw new InvalidDataException($"Expected guid, but got {read} bytes back for change vector");

                    changeVector[index].DbId = new Guid(buffer);
                    changeVector[index].Etag = iter.CreateReaderForCurrent().ReadBigEndianInt64();
                    index++;
                } while (iter.MoveNext());
            }
            return changeVector;
        }

        public ChangeVectorEntry[] GetChangeVector(DocumentsOperationContext context)
        {
            AssertTransaction(context);

            var tree = context.Transaction.InnerTransaction.CreateTree("ChangeVector");
            var changeVector = new ChangeVectorEntry[tree.State.NumberOfEntries];
            using (var iter = tree.Iterate(false))
            {
                if (iter.Seek(Slices.BeforeAllKeys) == false)
                    return changeVector;
                var buffer = new byte[16];
                int index = 0;
                do
                {
                    var read = iter.CurrentKey.CreateReader().Read(buffer, 0, 16);
                    if (read != 16)
                        throw new InvalidDataException($"Expected guid, but got {read} bytes back for change vector");

                    changeVector[index].DbId = new Guid(buffer);
                    changeVector[index].Etag = iter.CreateReaderForCurrent().ReadBigEndianInt64();
                    index++;
                } while (iter.MoveNext());
            }
            return changeVector;
        }

        public void SetChangeVector(DocumentsOperationContext context, ChangeVectorEntry[] changeVector)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree("ChangeVector");
            for (int i = 0; i < changeVector.Length; i++)
            {
                var entry = changeVector[i];
                tree.Add(Slice.External(context.Allocator, (byte*)&entry.DbId, (ushort)sizeof(Guid)),
                         Slice.External(context.Allocator, (byte*)&entry.Etag, (ushort)sizeof(long)));
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
            var table = new Table(_docsSchema, context.Transaction.InnerTransaction);

            var prefixSlice = GetSliceFromKey(context, prefix);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekByPrimaryKey(prefixSlice))
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
            var table = new Table(_docsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(_docsSchema.FixedSizeIndexes["AllDocsEtags"], long.MaxValue))
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
            var table = new Table(_docsSchema, "@" + collection, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(_docsSchema.FixedSizeIndexes["CollectionEtags"], long.MaxValue))
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
            var table = new Table(_docsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(_docsSchema.FixedSizeIndexes["AllDocsEtags"], etag))
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

        public IEnumerable<Document> GetDocumentsAfter(DocumentsOperationContext context, long etag)
        {
            var table = new Table(_docsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(_docsSchema.FixedSizeIndexes["AllDocsEtags"], etag))
            {
                yield return TableValueToDocument(context, result);
            }
        }

        public IEnumerable<Document> GetDocumentsAfter(DocumentsOperationContext context, string collection, long etag, int start, int take)
        {
            var collectionName = "@" + collection;
            if (context.Transaction.InnerTransaction.ReadTree(collectionName) == null)
                yield break;

            var table = new Table(_docsSchema, collectionName, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(_docsSchema.FixedSizeIndexes["CollectionEtags"], etag))
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
            var table = new Table(_docsSchema, context.Transaction.InnerTransaction);

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
                table = new Table(_tombstonesSchema, "#" + collection, context.Transaction.InnerTransaction);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                yield break;
            }

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(_tombstonesSchema.FixedSizeIndexes["CollectionEtags"], etag))
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
                table = new Table(_docsSchema, "@" + collection, context.Transaction.InnerTransaction);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return 0;
            }

            var result = table
                        .SeekBackwardFrom(_docsSchema.FixedSizeIndexes["CollectionEtags"], long.MaxValue)
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
                table = new Table(_tombstonesSchema, "#" + collection, context.Transaction.InnerTransaction);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return 0;
            }

            var result = table
                .SeekBackwardFrom(_tombstonesSchema.FixedSizeIndexes["CollectionEtags"], long.MaxValue)
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
                table = new Table(_tombstonesSchema, "#" + collection, context.Transaction.InnerTransaction);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return 0;
            }
            return table
                    .SeekBackwardFrom(_tombstonesSchema.FixedSizeIndexes["DeletedEtags"], etag)
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

            int size;
            var buffer = context.GetNativeTempBuffer(
                byteCount
                + sizeof(char) * key.Length // for the lower calls
                , out size);

            fixed (char* pChars = key)
            {
                var destChars = (char*)buffer;
                for (var i = 0; i < key.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }

                var keyBytes = buffer + key.Length * sizeof(char);

                size = Encoding.UTF8.GetBytes(destChars, key.Length, keyBytes, byteCount);
                return Slice.External(context.Allocator, keyBytes, (ushort)size);
            }
        }

        public static void GetLowerKeySliceAndStorageKey(JsonOperationContext context, string str, out byte* lowerKey, out int lowerSize,
            out byte* key, out int keySize)
        {
            var byteCount = Encoding.UTF8.GetMaxByteCount(str.Length);
            if (byteCount > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {byteCount} bytes. The invalid key is '{str}'.",
                    nameof(str));

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


            var jsonParserState = new JsonParserState();
            jsonParserState.FindEscapePositionsIn(str);
            var maxKeyLenSize = JsonParserState.VariableSizeIntSize(byteCount);
            var escapePositionsSize = jsonParserState.GetEscapePositionsSize();
            var buffer = context.GetNativeTempBuffer(
                sizeof(char) * str.Length // for the lower calls
                + byteCount // lower key
                + maxKeyLenSize // the size of var int for the len of the key
                + byteCount // actual key
                + escapePositionsSize
                , out lowerSize);

            fixed (char* pChars = str)
            {
                var destChars = (char*)buffer;
                for (var i = 0; i < str.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }

                lowerKey = buffer + str.Length * sizeof(char);

                lowerSize = Encoding.UTF8.GetBytes(destChars, str.Length, lowerKey, byteCount);

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
            result.Etag = IPAddress.NetworkToHostOrder(*(long*)ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);

            return result;
        }

        private static DocumentTombstone TableValueToTombstone(JsonOperationContext context, TableValueReader tvr)
        {
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

            return result;
        }

        public bool Delete(DocumentsOperationContext context, string key, long? expectedEtag)
        {
            return Delete(context, GetSliceFromKey(context, key), expectedEtag);
        }

        public bool Delete(DocumentsOperationContext context, Slice loweredKey, long? expectedEtag)
        {
            var doc = Get(context, loweredKey);
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

            if (doc.Etag == _lastEtag)
            {
                var etagTree = context.Transaction.InnerTransaction.ReadTree("Etags");
                var etag = _lastEtag;
                etagTree.Add(LastEtagSlice, Slice.External(context.Allocator, (byte*)&etag, sizeof(long)));
            }

            string originalCollectionName;
            bool isSystemDocument;
            var collectionName = GetCollectionName(loweredKey, doc.Data, out originalCollectionName, out isSystemDocument);
            var table = new Table(_docsSchema, collectionName, context.Transaction.InnerTransaction);

            CreateTombstone(context, table, doc, originalCollectionName);

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

        private void CreateTombstone(DocumentsOperationContext context, Table collectionDocsTable, Document doc, string collectionName)
        {
            int size;
            var ptr = collectionDocsTable.DirectRead(doc.StorageId, out size);
            var tvr = new TableValueReader(ptr, size);

            int lowerSize;
            var lowerKey = tvr.Read(0, out lowerSize);

            int keySize;
            var keyPtr = tvr.Read(2, out keySize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = IPAddress.HostToNetworkOrder(newEtag);
            var documentEtagBigEndian = IPAddress.HostToNetworkOrder(doc.Etag);

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize},
                {(byte*) &newEtagBigEndian , sizeof (long)},
                {(byte*) &documentEtagBigEndian , sizeof (long)},
                {keyPtr, keySize}
            };

            var col = "#" + collectionName; // TODO: We need a way to turn a string to a prefixed value that doesn't involve allocations
            _tombstonesSchema.Create(context.Transaction.InnerTransaction, col);
            var table = new Table(_tombstonesSchema, col, context.Transaction.InnerTransaction);

            table.Insert(tbv);
        }

        public PutResult Put(DocumentsOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Document key cannot be null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put",
                    nameof(context));

            string originalCollectionName;
            bool isSystemDocument;
            var collectionName = GetCollectionName(key, document, out originalCollectionName, out isSystemDocument);
            _docsSchema.Create(context.Transaction.InnerTransaction, collectionName);
            var table = new Table(_docsSchema, collectionName, context.Transaction.InnerTransaction);

            if (key[key.Length - 1] == '/')
            {
                key = GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, table, context);
            }

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = IPAddress.HostToNetworkOrder(newEtag);

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize}, //0
                {(byte*) &newEtagBigEndian , sizeof (long)}, //1
                {keyPtr, keySize}, //2
                {document.BasePointer, document.Size}, //3
            };

            var oldValue = table.ReadByKey(Slice.External(context.Allocator, lowerKey, (ushort)lowerSize));
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
                var oldCollectionName = Document.GetCollectionName(key, oldDoc, out isSystemDocument);
                if (oldCollectionName != originalCollectionName)
                    throw new InvalidOperationException(
                        $"Changing '{key}' from '{oldCollectionName}' to '{originalCollectionName}' via update is not supported.{System.Environment.NewLine}" +
                        $"Delete the document and recreate the document {key}.");

                table.Update(oldValue.Id, tbv);
            }

            if (isSystemDocument == false)
            {
                _documentDatabase.BundleLoader.VersioningStorage?.PutFromDocument(context, originalCollectionName, key, newEtagBigEndian, document);
                _documentDatabase.BundleLoader.ExpiredDocumentsCleaner?.Put(context, Slice.External(context.Allocator, lowerKey, (ushort)lowerSize), document);
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
            var fstIndex = _docsSchema.FixedSizeIndexes["AllDocsEtags"];
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
                var collectionTable = new Table(_docsSchema, collectionName, context.Transaction.InnerTransaction);

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
                table = new Table(_tombstonesSchema, "#" + collection, transaction);
            }
            catch (InvalidDataException)
            {
                // TODO [ppekrol] how to handle missing collection?
                return;
            }
            if (_log.IsDebugEnabled)
                _log.Debug($"Deleting tombstones earlier than {etag} in {collection}");
            table.DeleteBackwardFrom(_tombstonesSchema.FixedSizeIndexes["CollectionEtags"], etag, long.MaxValue);
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
    }
}