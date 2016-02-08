using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private readonly RavenConfiguration _configuration;
        private readonly TableSchema _docsSchema = new TableSchema();
        private readonly ILog _log;
        private readonly string _name;
        private static readonly Slice LastEtagSlice = "LastEtag";

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        public string DataDirectory;
        public ContextPool ContextPool;
        private UnmanagedBuffersPool _unmanagedBuffersPool;
        private const string NoCollectionSpecified = "Raven/Empty";
        private const string SystemDocumentsCollection = "Raven/SystemDocs";

        public DocumentsStorage(string name, RavenConfiguration configuration)
        {
            _name = name;
            _configuration = configuration;
            _log = LogManager.GetLogger(typeof (DocumentsStorage).FullName + "." + _name);

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
        }

        public StorageEnvironment Environment { get; private set; }

        public void Dispose()
        {
            Environment?.Dispose();
            Environment = null;
            _unmanagedBuffersPool?.Dispose();
            _unmanagedBuffersPool = null;
            ContextPool?.Dispose();
            ContextPool = null;
        }

        public void Initialize()
        {
            if (_log.IsDebugEnabled)
            {
                _log.Debug("Starting to open document storage for {0}", _configuration.Core.RunInMemory ? "<memory>" : _configuration.Core.DataDirectory);
            }
            var options = _configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(_configuration.Core.DataDirectory);

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
                _unmanagedBuffersPool = new UnmanagedBuffersPool(_name);
                ContextPool = new ContextPool(_unmanagedBuffersPool, Environment);

                using (var tx = Environment.WriteTransaction())
                {
                    tx.CreateTree("Docs");
                    tx.CreateTree("Identities");

                    _docsSchema.Create(tx, SystemDocumentsCollection);

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

        public static long ReadLastEtag(Transaction tx)
        {
            var tree = tx.CreateTree("Etags");
            var readResult = tree.Read(LastEtagSlice);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            var fst = new FixedSizeTree(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects, "AllDocsEtags",
                sizeof (long));

            using (var it = fst.Iterate())
            {
                if (it.SeekToLast())
                {
                    lastEtag = Math.Max(lastEtag, it.CurrentKey);
                }
            }
            return lastEtag;
        }

        public IEnumerable<Document> GetDocumentsStartingWith(RavenOperationContext context, string prefix, string matches, string exclude, int start, int take)
        {
            var table = new Table(_docsSchema, context.Transaction);

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

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(RavenOperationContext context, int start, int take)
        {
            var table = new Table(_docsSchema, context.Transaction);

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
        public IEnumerable<Document> GetDocumentsAfter(RavenOperationContext context, long etag, int start, int take)
        {
            var table = new Table(_docsSchema, context.Transaction);

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

        public IEnumerable<Document> GetDocumentsAfter(RavenOperationContext context, string collection, long etag, int start, int take)
        {
            var table = new Table(_docsSchema, "@"+collection, context.Transaction);

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

        public Document Get(RavenOperationContext context, string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put",
                    nameof(context));

            var table = new Table(_docsSchema, context.Transaction);

            var tvr = table.ReadByKey(GetSliceFromKey(context, key));
            if (tvr == null)
                return null;

            return TableValueToDocument(context, tvr);
        }

        private Slice GetSliceFromKey(RavenOperationContext context, string key)
        {
            var byteCount = Encoding.UTF8.GetMaxByteCount(key.Length);
            if (byteCount > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {byteCount} bytes. The invalid key is '{key}'.",
                    nameof(key));

            int size;
            var buffer = context.GetNativeTempBuffer(
                byteCount
                + sizeof (char)*key.Length // for the lower calls
                , out size);

            fixed (char* pChars = key)
            {
                var destChars = (char*) buffer;
                for (var i = 0; i < key.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }

                var keyBytes = buffer + key.Length*sizeof (char);

                size = Encoding.UTF8.GetBytes(destChars, key.Length, keyBytes, byteCount);
                return new Slice(keyBytes, (ushort) size);
            }
        }

        private void GetLowerKeySliceAndStorageKey(RavenOperationContext context, string str, out byte* lowerKey, out int lowerSize,
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
            var keyLenSize = JsonParserState.VariableSizeIntSize(byteCount);
            var escapePositionsSize = jsonParserState.GetEscapePositionsSize();
            var buffer = context.GetNativeTempBuffer(
                sizeof (char)*str.Length // for the lower calls
                + byteCount // lower key
                + keyLenSize // the size of var int for the len of the key
                + byteCount // actual key
                + escapePositionsSize
                , out lowerSize);

            fixed (char* pChars = str)
            {
                var destChars = (char*) buffer;
                for (var i = 0; i < str.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }

                lowerKey = buffer + str.Length*sizeof (char);

                lowerSize = Encoding.UTF8.GetBytes(destChars, str.Length, lowerKey, byteCount);

                key = buffer + str.Length*sizeof (char) + byteCount;
                var writePos = key;
                keySize = Encoding.UTF8.GetBytes(pChars, str.Length, writePos + keyLenSize, byteCount);
                JsonParserState.WriteVariableSizeInt(ref writePos, keySize);
                jsonParserState.WriteEscapePositionsTo(writePos + keySize);
                keySize += escapePositionsSize + keyLenSize;
            }
        }

        private static Document TableValueToDocument(RavenOperationContext context, TableValueReader tvr)
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
            result.Etag = IPAddress.NetworkToHostOrder(*(long*) ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);
            return result;
        }

        public bool Delete(RavenOperationContext context, string key, long? expectedEtag)
        {
            var doc = Get(context, key);
            if (doc == null)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException(
                        $"Document {key} does not exists, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
                return false;
            }
            if (expectedEtag != null && doc.Etag != expectedEtag)
            {
                throw new ConcurrencyException(
                    $"Document {key} has etag {doc.Etag}, but Delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
            }

            if (doc.Etag == _lastEtag)
            {
                var etagTree = context.Transaction.ReadTree("Etags");
                var etag = _lastEtag;
                etagTree.Add(LastEtagSlice, new Slice((byte*) &etag, sizeof (long)));
            }
            var collectionName = GetCollectionName(key, doc.Data);
            var table = new Table(_docsSchema, collectionName, context.Transaction);
            table.Delete(doc.StorageId);

            return true;
        }

        public long Put(RavenOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put",
                    nameof(context));

            var collectionName = GetCollectionName(key, document);
            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var newEtag = ++_lastEtag;
            var newEtagBigEndian = IPAddress.HostToNetworkOrder(newEtag); 

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize},
                {(byte*) &newEtagBigEndian , sizeof (long)},
                {keyPtr, keySize},
                {document.BasePointer, document.Size}
            };

            _docsSchema.Create(context.Transaction, collectionName);

            var table = new Table(_docsSchema, collectionName, context.Transaction);
            var oldValue = table.ReadByKey(new Slice(lowerKey, (ushort) lowerSize));
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
                table.Update(oldValue.Id, tbv);
            }
            return newEtag;
        }

        private static string GetCollectionName(string key, BlittableJsonReaderObject document)
        {
            string collectionName;
            BlittableJsonReaderObject metadata;
            if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
            {
                collectionName = SystemDocumentsCollection;
            }
            else if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.RavenEntityName, out collectionName) == false)
            {
                collectionName = NoCollectionSpecified;
            }
            
            // we have to have some way to distinguish between dynamic tree names
            // and our fixed ones, otherwise a collection call Docs will corrupt our state
            return "@" + collectionName;
        }

        public long IdentityFor(RavenOperationContext ctx, string key)
        {
            var identities = ctx.Transaction.ReadTree("Identities");
            return identities.Increment(key, 1);
        }

        public long GetNumberOfDocuments(RavenOperationContext context)
        {
            var fst = context.Transaction.FixedTreeFor(_docsSchema.FixedSizeIndexes["AllDocsEtags"].NameAsSlice);
            return fst.NumberOfEntries;
        }

        public class CollectionStat
        {
            public string Name;
            public long Count;
        }

        public IEnumerable<CollectionStat> GetCollections(RavenOperationContext context)
        {
            using (var it = context.Transaction.LowLevelTransaction.RootObjects.Iterate())
            {
                if (it.Seek(Slice.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    if(context.Transaction.GetRootObjectType(it.CurrentKey) != RootObjectType.VariableSizeTree)
                        continue;

                    if (it.CurrentKey[0] != '@') // collection prefix
                        continue;

                    var collectionTableName = it.CurrentKey.ToString();
                    var collectionTable = new Table(_docsSchema, collectionTableName,context.Transaction);
                   

                    yield return new CollectionStat
                    {
                        Name = collectionTableName.Substring(1),
                        Count = collectionTable.NumberOfEntries
                    };
                } while (it.MoveNext());
            }
        }
    }
}