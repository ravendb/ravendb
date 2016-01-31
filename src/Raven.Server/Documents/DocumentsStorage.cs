using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Utils;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private readonly IConfigurationRoot _config;
        private readonly TableSchema _docsSchema = new TableSchema();
        private readonly ILog _log;
        private readonly string _name;
        private static readonly Slice LastEtagSlice = "LastEtag";

        /// <summary>
        ///     We don't need to actually modify this using thread safe code, since we can rely
        ///     on the tx lock to ensure no concurrent access
        /// </summary>
        private long _lastEtag;

        public string DataDirectory;

        public DocumentsStorage(string name, IConfigurationRoot config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _name = name;
            _config = config;
            _log = LogManager.GetLogger(typeof (DocumentsStorage).FullName + "." + _name);

            // The documents schema is as follows
            // 3 fields (lowered key, etag, key, document)
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
        }

        public void Initialize()
        {
            var runInMemory = _config.Get<bool>("run.in.memory");
            if (runInMemory == false)
            {
                DataDirectory = _config.Get<string>("system.path").ToFullPath();
            }
            if (_log.IsDebugEnabled)
            {
                _log.Debug("Starting to open document storage for {0}", (runInMemory ? "<memory>" : DataDirectory));
            }
            var options = runInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(DataDirectory);

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
                using (var tx = Environment.WriteTransaction())
                {
                    tx.CreateTree("Docs");
                    ReadLastEtag(tx);

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
                throw;
            }
        }

        public void ReadLastEtag(Transaction tx)
        {
            var tree = tx.CreateTree("Etags");
            var readResult = tree.Read(LastEtagSlice);
            if (readResult != null)
                _lastEtag = readResult.Reader.ReadLittleEndianInt64();

            var fst = new FixedSizeTree(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects, "AllDocsEtags",
                sizeof (long));

            using (var it = fst.Iterate())
            {
                if (it.SeekToLast())
                {
                    _lastEtag = Math.Max(_lastEtag, it.CurrentKey);
                }
            }
        }

        public IEnumerable<Document> GetDocumentsStartingWith(RavenOperationContext context, string prefix)
        {
            var table = new Table(_docsSchema, context.Transaction);

            var prefixSlice = GetSliceFromKey(context, prefix);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekByPrimaryKey(prefixSlice))
            {
                var document = TableValueToDocument(context, result);
                if (document.Key.StartsWith(prefix) == false)
                    break;
                yield return document;
            }
        }

        public IEnumerable<Document> GetDocumentsAfter(RavenOperationContext context, long etag)
        {
            var table = new Table(_docsSchema, context.Transaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekTo(_docsSchema.FixedSizeIndexes["AllDocsEtags"], etag))
            {
                yield return TableValueToDocument(context, result);
            }
        }

        public IEnumerable<Document> GetDocumentsAfter(RavenOperationContext context, string collection, long etag)
        {
            var table = new Table(_docsSchema, collection, context.Transaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekTo(_docsSchema.FixedSizeIndexes["CollectionEtags"], etag))
            {
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

        private void GetSliceFromKey(RavenOperationContext context, string str, out byte* lowerKey, out int lowerSize,
            out byte* key, out int keySize)
        {
            var byteCount = Encoding.UTF8.GetMaxByteCount(str.Length);
            if (byteCount > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {byteCount} bytes. The invalid key is '{str}'.",
                    nameof(str));


            var buffer = context.GetNativeTempBuffer(
                (byteCount*2)
                + sizeof (char)*str.Length // for the lower calls
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
                keySize = Encoding.UTF8.GetBytes(pChars, str.Length, key, byteCount);
            }
        }

        private static Document TableValueToDocument(RavenOperationContext context, TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id
            };
            int size;
            var ptr = tvr.Read(2, out size);
            result.Key = Encoding.UTF8.GetString(ptr, size);
            ptr = tvr.Read(1, out size);
            result.Etag = IPAddress.NetworkToHostOrder(*(long*) ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);
            return result;
        }

        public void Delete(RavenOperationContext context, string key, long? expectedEtag)
        {
            var doc = Get(context, key);
            if (doc == null)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException(
                        $"Document {key} does not exists, but delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
                return;
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
            var collectionName = GetCollectionName(doc.Data);
            var table = new Table(_docsSchema, collectionName, context.Transaction);
            table.Delete(doc.StorageId);
        }

        public long Put(RavenOperationContext context, string key, long? expectedEtag,
            BlittableJsonReaderObject document)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put",
                    nameof(context));

            var collectionName = GetCollectionName(document);
            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            GetSliceFromKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

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

        private static string GetCollectionName(BlittableJsonReaderObject document)
        {
            BlittableJsonReaderObject metadata;
            string collectionName;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.RavenEntityName, out collectionName) == false)
            {
                collectionName = "<no-collection>";
            }
            return collectionName;
        }
    }
}