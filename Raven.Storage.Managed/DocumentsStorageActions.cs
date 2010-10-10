using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
    public class DocumentsStorageActions : IDocumentStorageActions
    {
        private readonly TableStorage storage;
        private readonly ITransactionStorageActions transactionStorageActions;

        public DocumentsStorageActions(TableStorage storage, ITransactionStorageActions transactionStorageActions)
        {
            this.storage = storage;
            this.transactionStorageActions = transactionStorageActions;
        }

        public Tuple<int, int> FirstAndLastDocumentIds()
        {
            var lastOrDefault = storage.Documents["ById"].LastOrDefault();
            var last = 0;
            if (lastOrDefault != null)
                last = lastOrDefault.Value<int>("id");

            var firstOrDefault = storage.Documents["ById"].FirstOrDefault();
            var first = 0;
            if (firstOrDefault != null)
                first= firstOrDefault.Value<int>("id");
            return new Tuple<int, int>(first,last );
        }

        public IEnumerable<Tuple<JsonDocument, int>> DocumentsById(int startId, int endId)
        {
            var results = storage.Documents["ById"].SkipAfter(new JObject{{"id", startId}})
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
            var resultInTx = storage.DocumentsModifiedByTransactions.Read(new JObject { { "key", key } });
            if (transactionInformation != null && resultInTx != null)
            {
               if(new Guid(resultInTx.Key.Value<byte[]>("txId")) == transactionInformation.Id)
                {
                    if (resultInTx.Key.Value<bool>("deleted"))
                        return null;

                    JObject metadata = null;
                    JObject dataAsJson = null;
                    if (resultInTx.Position != -1)
                    {
                        using (var memoryStreamFromTx = new MemoryStream(resultInTx.Data()))
                        {
                            metadata = (JObject) JToken.ReadFrom(new BsonReader(memoryStreamFromTx));
                            dataAsJson = (JObject) JToken.ReadFrom(new BsonReader(memoryStreamFromTx));
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

            var memoryStream = new MemoryStream(readResult.Data());
            return new JsonDocument
            {
                Key = readResult.Key.Value<string>("key"),
                Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
                Metadata = (JObject)JToken.ReadFrom(new BsonReader(memoryStream)),
                DataAsJson = (JObject)JToken.ReadFrom(new BsonReader(memoryStream)),
                LastModified = readResult.Key.Value<DateTime>("modified"),
                NonAuthoritiveInformation = resultInTx != null
            };
        }

        public bool DeleteDocument(string key, Guid? etag, out JObject metadata)
        {
            AssertValidEtag(key, etag, "DELETE", null);

            metadata = null;
            var readResult = storage.Documents.Read(new JObject{{"key",key}});
            if (readResult == null)
                return false;

            metadata = (JObject)JToken.ReadFrom(new BsonReader(new MemoryStream(readResult.Data())));

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

            metadata.WriteTo(new BsonWriter(ms));
            data.WriteTo(new BsonWriter(ms));

            var lastOrefaultKeyById = storage.Documents["ById"].LastOrDefault();
            int id = 0;
            if (lastOrefaultKeyById != null)
                id = lastOrefaultKeyById.Value<int>("id");

            var newEtag = DocumentDatabase.CreateSequentialUuid();
            storage.Documents.Put(new JObject
            {
                {"key", key},
                {"etag", newEtag.ToByteArray()},
                {"modified", DateTime.UtcNow},
                {"id", id}
            },ms.ToArray());

            return newEtag;
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