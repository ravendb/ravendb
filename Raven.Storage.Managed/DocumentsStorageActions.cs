using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Http;
using Raven.Http.Exceptions;
using Raven.Storage.Managed.Impl;
using System.Linq;
using Raven.Database.Extensions;

namespace Raven.Storage.Managed
{
    public class DocumentsStorageActions : IDocumentStorageActions
    {
        private readonly TableStorage storage;
        private readonly ITransactionStorageActions transactionStorageActions;
        private readonly IUuidGenerator generator;
        private readonly IEnumerable<AbstractDocumentCodec> documentCodecs;

        public DocumentsStorageActions(TableStorage storage, ITransactionStorageActions transactionStorageActions, IUuidGenerator generator, IEnumerable<AbstractDocumentCodec> documentCodecs)
        {
            this.storage = storage;
            this.transactionStorageActions = transactionStorageActions;
            this.generator = generator;
            this.documentCodecs = documentCodecs;
        }

        public Tuple<int, int> FirstAndLastDocumentIds()
        {
            int last = GetLastDocumentId();

            int first = GetFirstDocumentId();
            return new Tuple<int, int>(first,last );
        }

        private int GetFirstDocumentId()
        {
            var firstOrDefault = storage.Documents["ById"].FirstOrDefault();
            var first = 0;
            if (firstOrDefault != null)
                first= firstOrDefault.Value<int>("id");
            return first;
        }

        private int GetLastDocumentId()
        {
            var lastOrDefault = storage.Documents["ById"].LastOrDefault();
            var last = 0;
            if (lastOrDefault != null)
                last = lastOrDefault.Value<int>("id");
            return last;
        }

        public IEnumerable<Tuple<JsonDocument, int>> DocumentsById(int startId, int endId)
        {
            var results = storage.Documents["ById"].SkipTo(new JObject{{"id", startId}})
                .TakeWhile(x=>x.Value<int>("id") <= endId);

            foreach (var result in results)
            {
                yield return new Tuple<JsonDocument, int>(
                    DocumentByKey(result.Value<string>("key"), null),
                    result.Value<int>("id")
                    );
            }
        }

        public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start)
        {
            return storage.Documents["ByEtag"].SkipFromEnd(start)
                .Select(result => DocumentByKey(result.Value<string>("key"), null));
        }

        public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag)
        {
            return storage.Documents["ByEtag"].SkipAfter(new JObject {{"etag", etag.ToByteArray()}})
                .Select(result => DocumentByKey(result.Value<string>("key"), null));
        }

        public long GetDocumentsCount()
        {
            return storage.Documents["ByKey"].Count;
        }

        public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
        {
            JObject metadata = null;
            JObject dataAsJson = null;
            
            var resultInTx = storage.DocumentsModifiedByTransactions.Read(new JObject { { "key", key } });
            if (transactionInformation != null && resultInTx != null)
            {
               if(new Guid(resultInTx.Key.Value<byte[]>("txId")) == transactionInformation.Id)
                {
                    if (resultInTx.Key.Value<bool>("deleted"))
                        return null;

                   if (resultInTx.Position != -1)
                    {
                        var bufferFromTx = resultInTx.Data();
                        using (var memoryStreamFromTx = new MemoryStream(bufferFromTx, 0, bufferFromTx.Length, writable: false, publiclyVisible: true))
                        {
                            ReadMetadataAndData(key, memoryStreamFromTx, out metadata, out dataAsJson);
                        }
                    }
                    return new JsonDocument
                    {
                        Key = resultInTx.Key.Value<string>("key"),
                        Etag = new Guid(resultInTx.Key.Value<byte[]>("etag")),
                        Metadata = metadata,
                        DataAsJson = dataAsJson,
                        LastModified = resultInTx.Key.Value<DateTime>("modified"),
                    };
                }
            }

            var readResult = storage.Documents.Read(new JObject{{"key", key}});
            if (readResult == null)
                return null;

            var buffer = readResult.Data();
            var memoryStream = new MemoryStream(buffer, 0, buffer.Length, writable: false, publiclyVisible: true);
            ReadMetadataAndData(key, memoryStream, out metadata, out dataAsJson);
            return new JsonDocument
            {
                Key = readResult.Key.Value<string>("key"),
                Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
                Metadata = metadata,
                DataAsJson = dataAsJson,
                LastModified = readResult.Key.Value<DateTime>("modified"),
                NonAuthoritiveInformation = resultInTx != null
            };
        }

        private void ReadMetadataAndData(string key, MemoryStream memoryStreamFromTx, out JObject metadata, out JObject dataAsJson)
        {
            metadata = memoryStreamFromTx.ToJObject();
            var metadataCopy = metadata;
            var dataBuffer = new byte[memoryStreamFromTx.Length - memoryStreamFromTx.Position];
            Buffer.BlockCopy(memoryStreamFromTx.GetBuffer(), (int)memoryStreamFromTx.Position, dataBuffer, 0,
                             dataBuffer.Length);
            documentCodecs.Aggregate(dataBuffer, (bytes, codec) => codec.Decode(key, metadataCopy, bytes));
            dataAsJson = dataBuffer.ToJObject();
        }

        public bool DeleteDocument(string key, Guid? etag, out JObject metadata)
        {
            AssertValidEtag(key, etag, "DELETE", null);

            metadata = null;
            var readResult = storage.Documents.Read(new JObject{{"key",key}});
            if (readResult == null)
                return false;

            metadata = readResult.Data().ToJObject();

            storage.Documents.Remove(new JObject
            {
                {"key", key},
            });

            return true;
        }

        public Guid AddDocument(string key, Guid? etag, JObject data, JObject metadata)
        {
            AssertValidEtag(key, etag, "PUT", null);

            var ms = new MemoryStream();

            metadata.WriteTo(ms);

            var bytes = documentCodecs.Aggregate(data.ToBytes(), (current, codec) => codec.Encode(key, data, metadata, current));

            ms.Write(bytes, 0, bytes.Length);

            var newEtag = generator.CreateSequentialUuid();
            storage.Documents.Put(new JObject
            {
                {"key", key},
                {"etag", newEtag.ToByteArray()},
                {"modified", DateTime.UtcNow},
                {"id", IncrementLastDocumentId()},
                {"entityName", metadata.Value<string>("Raven-Entity-Name")}
            },ms.ToArray());

            return newEtag;
        }

        private int? lastDocumentId;
        private int IncrementLastDocumentId()
        {
            if(lastDocumentId != null)
            {
                lastDocumentId = lastDocumentId.Value + 1;
                return lastDocumentId.Value;
            }
            var lastOrefaultKeyById = storage.Documents["ById"].LastOrDefault();
            int id = 1;
            if (lastOrefaultKeyById != null)
                id = lastOrefaultKeyById.Value<int>("id") + 1;
            lastDocumentId = id;
            return id;
        }

        private void AssertValidEtag(string key, Guid? etag, string op, TransactionInformation transactionInformation)
        {
            var readResult = storage.Documents.Read(new JObject
            {
                {"key", key},
            });

            if (readResult != null)
            {
                StorageHelper.AssertNotModifiedByAnotherTransaction(storage, transactionStorageActions, key, readResult, transactionInformation);

                if (etag != null)
                {
                    var existingEtag = new Guid(readResult.Key.Value<byte[]>("etag"));
                    if (existingEtag != etag)
                    {
                        throw new ConcurrencyException(op + " attempted on document '" + key +
                                                       "' using a non current etag")
                        {
                            ActualETag = etag.Value,
                            ExpectedETag = existingEtag
                        };
                    }
                }
            }
            else
            {
                readResult = storage.DocumentsModifiedByTransactions.Read(new JObject
                {
                    {"key", key}
                });
                StorageHelper.AssertNotModifiedByAnotherTransaction(storage, transactionStorageActions, key, readResult, transactionInformation);

            }
        }

    }
}