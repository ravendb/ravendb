using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;
using System.Linq;
using Raven.Database.Json;

namespace Raven.Storage.Managed
{
    public class TransactionStorageActions : ITransactionStorageActions
    {
        private readonly TableStorage storage;

        public TransactionStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public Guid AddDocumentInTransaction(string key, Guid? etag, JObject data, JObject metadata, TransactionInformation transactionInformation)
        {
            var readResult = storage.Documents.Read(new JObject { { "key", key } });
            if (readResult != null) // update
            {
                StorageHelper.AssertNotModifiedByAnotherTransaction(storage, this, key, readResult, transactionInformation);
                AssertValidEtag(key, readResult, storage.DocumentsModifiedByTransactions.Read(new JObject{{"key", key}}), etag);

                readResult.Key["txId"] = transactionInformation.Id.ToByteArray();
                if (storage.Documents.UpdateKey(readResult.Key) == false)
                    throw new ConcurrencyException("PUT attempted on document '" + key +
                                                   "' that is currently being modified by another transaction");
            }
            else
            {
                readResult = storage.DocumentsModifiedByTransactions.Read(new JObject { { "key", key } });
                StorageHelper.AssertNotModifiedByAnotherTransaction(storage, this, key, readResult, transactionInformation);
            }

            storage.Transactions.UpdateKey(new JObject
            {
                {"txId", transactionInformation.Id.ToByteArray()},
                {"timeout", DateTime.UtcNow.Add(transactionInformation.Timeout)}
            });

            var ms = new MemoryStream();

            metadata.WriteTo(ms);
            data.WriteTo(ms);

            var newEtag = DocumentDatabase.CreateSequentialUuid();
            storage.DocumentsModifiedByTransactions.Put(new JObject
            {
                {"key", key},
                {"etag", newEtag.ToByteArray()},
                {"modified", DateTime.UtcNow},
                {"txId", transactionInformation.Id.ToByteArray()}
            }, ms.ToArray());

            return newEtag;
        }

        private static void AssertValidEtag(string key, PersistentDictionary.ReadResult doc, PersistentDictionary.ReadResult docInTx, Guid? etag)
        {
            if (doc == null)
                return;
            var existingEtag =
                docInTx != null
                    ? new Guid(docInTx.Key.Value<byte[]>("etag"))
                    : new Guid(doc.Key.Value<byte[]>("etag"));


            if (etag != null && etag.Value != existingEtag)
            {
                throw new ConcurrencyException("PUT attempted on document '" + key +
                                               "' using a non current etag")
                {
                    ActualETag = etag.Value,
                    ExpectedETag = existingEtag
                };
            }
        }

        public void DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
        {
            var readResult = storage.Documents.Read(new JObject { { "key", key } });
            if (readResult == null)
            {
                return;
            }
            readResult = storage.DocumentsModifiedByTransactions.Read(new JObject { { "key", key } });
            StorageHelper.AssertNotModifiedByAnotherTransaction(storage, this, key, readResult, transactionInformation);
            AssertValidEtag(key, readResult, storage.DocumentsModifiedByTransactions.Read(new JObject{{"key", key}}), etag);

            if (readResult != null)
            {
                readResult.Key["txId"] = transactionInformation.Id.ToByteArray();
                if (storage.Documents.UpdateKey(readResult.Key) == false)
                    throw new ConcurrencyException("DELETE attempted on document '" + key +
                                                   "' that is currently being modified by another transaction");
            }

            storage.Transactions.UpdateKey(new JObject
            {
                {"txId", transactionInformation.Id.ToByteArray()},
                {"timeout", DateTime.UtcNow.Add(transactionInformation.Timeout)}
            });

            var newEtag = DocumentDatabase.CreateSequentialUuid();
            storage.DocumentsModifiedByTransactions.UpdateKey(new JObject
            {
                {"key", key},
                {"etag", newEtag.ToByteArray()},
                {"modified", DateTime.UtcNow},
                {"deleted", true},
                {"txId", transactionInformation.Id.ToByteArray()}
            });
        }

        public void RollbackTransaction(Guid txId)
        {
            CompleteTransaction(txId, data =>
            {
                var readResult = storage.Documents.Read(new JObject { { "key", data.Key } });
                if (readResult == null)
                    return;
                ((JObject)readResult.Key).Remove("txId");
                storage.Documents.UpdateKey(readResult.Key);
            });
        }

        public void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout)
        {
            storage.Transactions.UpdateKey(new JObject
            {
                {"txId", toTxId.ToByteArray()},
                {"timeout", DateTime.UtcNow.Add(timeout)}
            });

            var transactionInformation = new TransactionInformation { Id = toTxId, Timeout = timeout };
            CompleteTransaction(fromTxId, data =>
            {
                var readResult = storage.Documents.Read(new JObject { { "key", data.Key } });
                if (readResult != null)
                {
                    ((JObject)readResult.Key)["txId"] = toTxId.ToByteArray();
                    storage.Documents.UpdateKey(readResult.Key);
                }

                if (data.Delete)
                    DeleteDocumentInTransaction(transactionInformation, data.Key, null);
                else
                    AddDocumentInTransaction(data.Key, null, data.Data, data.Metadata, transactionInformation);
            });
        }

        public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
        {
            storage.Transactions.Remove(new JObject { { "txId", txId.ToByteArray() } });

            var documentsInTx = storage.DocumentsModifiedByTransactions["ByTxId"]
                .SkipTo(new JObject { { "txId", txId.ToByteArray() } })
                .TakeWhile(x => new Guid(x.Value<byte[]>("txId")) == txId);

            foreach (var docInTx in documentsInTx)
            {
                var readResult = storage.DocumentsModifiedByTransactions.Read(docInTx);

                storage.DocumentsModifiedByTransactions.Remove(docInTx);

                JObject metadata = null;
                JObject data = null;
                if (readResult.Position > 0) // position can never be 0, because of the skip record
                {
                    var ms = new MemoryStream(readResult.Data());
                    metadata = ms.ToJObject();
                    data = ms.ToJObject();
                }
                perDocumentModified(new DocumentInTransactionData
                {
                    Key = readResult.Key.Value<string>("key"),
                    Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
                    Delete = readResult.Key.Value<bool>("deleted"),
                    Metadata = metadata,
                    Data = data,
                });

            }
        }

        public IEnumerable<Guid> GetTransactionIds()
        {
            return storage.Transactions.Keys.Select(x => new Guid(x.Value<byte[]>("txId")));
        }
    }
}