using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DocumentDatabase));

        private readonly TableSchema _docsSchema = new TableSchema();

        private readonly VersioningConfiguration _versioningConfiguration;

        private const string RevisionDocuments = "RevisionDocuments";
        private const string RevisionsCount = "RevisionsCount";

        private readonly VersioningConfigurationCollection _emptyConfiguration = new VersioningConfigurationCollection();

        private VersioningStorage(DocumentDatabase database, VersioningConfiguration versioningConfiguration)
        {
            _versioningConfiguration = versioningConfiguration;

            // The documents schema is as follows
            // 5 fields (lowered key, recored separator, etag, lazy string key, document)
            // We are you using the record separator in order to avoid loading another documents that has the same key prefix, 
            //      e.g. fitz(record-separator)01234567 and fitz0(record-separator)01234567, without the record separator we would have to load also fitz0 and filter it.
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            _docsSchema.DefineIndex("KeyAndEtag", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 3,
            });
            _docsSchema.DefineFixedSizeIndex("Etag", new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 2,
            });

            using (var tx = database.DocumentsStorage.Environment.WriteTransaction())
            {
                tx.CreateTree(RevisionDocuments);
                _docsSchema.Create(tx, RevisionDocuments);

                tx.CreateTree(RevisionsCount);

                tx.Commit();
            }
        }

        public static VersioningStorage LoadConfigurations(DocumentDatabase database)
        {
            DocumentsOperationContext context;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var configuration = database.DocumentsStorage.Get(context, Constants.Versioning.RavenVersioningConfiguration);
                if (configuration == null)
                    return null;

                try
                {
                    var versioningConfiguration = JsonDeserialization.VersioningConfiguration(configuration.Data);
                    return new VersioningStorage(database, versioningConfiguration);
                }
                catch (Exception e)
                {
                    //TODO: This should generate an alert, so admin will know that something is very bad
                    //TODO: Or this should throw and we should have a config flag to ignore the error
                    if (Log.IsWarnEnabled)
                        Log.WarnException($"Cannot enable versioning for documents as the versioning configuration document {Constants.Versioning.RavenVersioningConfiguration} is not valid: {configuration.Data}", e);
                    return null;
                }
            }
        }

        private VersioningConfigurationCollection GetVersioningConfiguration(string collectionName)
        {
            VersioningConfigurationCollection configuration;
            if (_versioningConfiguration.Collections != null && _versioningConfiguration.Collections.TryGetValue(collectionName, out configuration))
            {
                return configuration;
            }

            if (_versioningConfiguration.Default != null)
            {
                return _versioningConfiguration.Default;
            }

            return _emptyConfiguration;
        }

        public void PutFromDocument(DocumentsOperationContext context, string collectionName, string key, long newEtagBigEndian, BlittableJsonReaderObject document)
        {
            var enableVersioning = false;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata, out metadata))
            {
                bool disableVersioning;
                if (metadata.TryGet(Constants.Versioning.RavenDisableVersioning, out disableVersioning))
                {
                    DynamicJsonValue mutatedMetadata;
                    Debug.Assert(metadata.Modifications == null);
                   
                    metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
                    mutatedMetadata.Remove(Constants.Versioning.RavenDisableVersioning);
                    if (disableVersioning)
                        return;
                }

                if (metadata.TryGet(Constants.Versioning.RavenEnableVersioning, out enableVersioning))
                {
                    DynamicJsonValue mutatedMetadata = metadata.Modifications;
                    if (mutatedMetadata == null)
                        metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
                    mutatedMetadata.Remove(Constants.Versioning.RavenEnableVersioning);
                }
            }

            var configuration = GetVersioningConfiguration(collectionName);
            if (enableVersioning == false && configuration.Active == false)
                return;

            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);
            var prefixSlice = GetSliceFromKey(context, key);
            var revisionsCount = IncrementCountOfRevisions(context, prefixSlice, 1);
            DeleteOldRevisions(context, table, prefixSlice, configuration.MaxRevisions, revisionsCount);

            PutInternal(context, key, newEtagBigEndian, document, table);
        }

        public void PutDirect(DocumentsOperationContext context, string key, long etag, BlittableJsonReaderObject document)
        {
            var newEtagBigEndian = IPAddress.HostToNetworkOrder(etag);

            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);
            PutInternal(context, key, newEtagBigEndian, document, table);
        }

        private void PutInternal(DocumentsOperationContext context, string key, long newEtagBigEndian, BlittableJsonReaderObject document, Table table)
        {
            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentsStorage.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            byte recordSeperator = 30;

            var tbv = new TableValueBuilder
            {
                {lowerKey, lowerSize},
                {&recordSeperator, sizeof(char)},
                {(byte*)&newEtagBigEndian, sizeof(long)},
                {keyPtr, keySize},
                {document.BasePointer, document.Size}
            };

            table.Insert(tbv);
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, long? maxRevisions, long revisionsCount)
        {
            if (maxRevisions.HasValue == false || maxRevisions.Value == int.MaxValue)
                return;

            var numberOfRevisionsToDelete = revisionsCount - maxRevisions.Value;
            if (numberOfRevisionsToDelete <= 0)
                return;

            var deletedRevisionsCount = table.DeleteForwardFrom(_docsSchema.Indexes["KeyAndEtag"], prefixSlice, numberOfRevisionsToDelete);
            Debug.Assert(numberOfRevisionsToDelete == deletedRevisionsCount);
            IncrementCountOfRevisions(context, prefixSlice, -deletedRevisionsCount);
        }

        private long IncrementCountOfRevisions(DocumentsOperationContext context, Slice prefixedLoweredKey, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCount);
            return numbers.Increment(prefixedLoweredKey, delta);
        }

        private void DeleteCountOfRevisions(DocumentsOperationContext context, Slice prefixedLoweredKey)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCount);
            numbers.Delete(prefixedLoweredKey);
        }

        public void Delete(DocumentsOperationContext context, string collectionName, Slice loweredKey)
        {
            Debug.Assert(collectionName[0] != '@');
            
            var configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            if (configuration.PurgeOnDelete == false)
                return;

            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);
            var prefixKeyMem = context.Allocator.Allocate(loweredKey.Size +1);
            loweredKey.CopyTo(0, prefixKeyMem.Ptr, 0, loweredKey.Size);
            prefixKeyMem.Ptr[loweredKey.Size] = (byte)30; // the record separator
            var prefixSlice = new Slice(SliceOptions.Key, prefixKeyMem);
            table.DeleteForwardFrom(_docsSchema.Indexes["KeyAndEtag"], prefixSlice, long.MaxValue);
            DeleteCountOfRevisions(context, prefixSlice);
        }

        public IEnumerable<Document> GetRevisions(DocumentsOperationContext context, string key, int start, int take)
        {
            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);

            var prefixSlice = GetSliceFromKey(context, key);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var sr in table.SeekForwardFrom(_docsSchema.Indexes["KeyAndEtag"], prefixSlice, startsWith: true))
            {
                foreach (var tvr in sr.Results)
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    if (take-- <= 0)
                        yield break;

                    var document = TableValueToDocument(context, tvr);
                    yield return document;
                }
                if (take <= 0)
                    yield break;
            }
        }

        public IEnumerable<Document> GetRevisionsAfter(DocumentsOperationContext context, long etag)
        {
            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);

            foreach (var tvr in table.SeekForwardFrom(_docsSchema.FixedSizeIndexes["Etag"], etag))
            {
                var document = TableValueToDocument(context, tvr);
                yield return document;
            }
        }

        public IEnumerable<Document> GetRevisionsAfter(DocumentsOperationContext context, long etag, int take)
        {
            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);

            foreach (var tvr in table.SeekForwardFrom(_docsSchema.FixedSizeIndexes["Etag"], etag))
            {
                var document = TableValueToDocument(context, tvr);
                yield return document;

                if (take-- <= 0)
                    yield break;
            }
        }

        public static Slice GetSliceFromKey(DocumentsOperationContext context, string key)
        {
            // TODO: Is this needed? Could we just use the ByteString?

            var byteCount = Encoding.UTF8.GetMaxByteCount(key.Length);
            if (byteCount > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {byteCount} bytes. The invalid key is '{key}'.",
                    nameof(key));

            int size;
            var buffer = context.GetNativeTempBuffer(
                byteCount
                + sizeof(char) * key.Length // for the lower calls
                + sizeof(char) * 2 // for the record separator
                , out size);

            fixed (char* pChars = key)
            {
                var destChars = (char*)buffer;
                for (var i = 0; i < key.Length; i++)
                {
                    destChars[i] = char.ToLowerInvariant(pChars[i]);
                }
                destChars[key.Length] = (char)30; // the record separator

                var keyBytes = buffer + sizeof(char) + key.Length * sizeof(char);

                size = Encoding.UTF8.GetBytes(destChars, key.Length + 1, keyBytes, byteCount + 1);
                return Slice.External(context.Allocator, keyBytes, (ushort)size);
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
            var ptr = tvr.Read(3, out size);
            byte offset;
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);
            ptr = tvr.Read(2, out size);
            result.Etag = IPAddress.NetworkToHostOrder(*(long*)ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read(4, out size), size, context);

            return result;
        }

        public long GetNumberOfRevisionDocuments(DocumentsOperationContext context)
        {
            var table = new Table(_docsSchema, RevisionDocuments, context.Transaction.InnerTransaction);
            return table.GetNumberEntriesFor(_docsSchema.FixedSizeIndexes["Etag"]);
        }
    }
}