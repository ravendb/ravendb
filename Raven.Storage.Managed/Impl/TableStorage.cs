using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Storage.Managed.Impl
{
    /// <summary>
    /// Give us nice names for the persistent dictionaries
    /// 0 - Details
    /// 1 - Identity
    /// 2 - Attachments
    /// </summary>
    public class TableStorage : AggregateDictionary
    {
        private readonly ThreadLocal<Guid> txId = new ThreadLocal<Guid>(() => Guid.Empty);

        public TableStorage(IPersistentSource persistentSource)
            : base(persistentSource)
        {
            Details = new PersistentDictionaryAdapter(txId,
                                                      Add(new PersistentDictionary(persistentSource,
                                                                                   JTokenComparer.Instance)));
            Identity = new PersistentDictionaryAdapter(txId,
                                                       Add(new PersistentDictionary(persistentSource, new ModifiedJTokenComparer(x=>x.Value<string>("name")))));

            PersistentDictionary attachmentPersistentDictionary =
                Add(new PersistentDictionary(persistentSource, new ModifiedJTokenComparer(x => x.Value<string>("key"))));
            Attachments = new PersistentDictionaryAdapter(txId, attachmentPersistentDictionary)
            {
                {"ByEtag", attachmentPersistentDictionary.AddSecondaryIndex(x => x.Value<byte[]>("etag"))}
            };

            PersistentDictionary documentsPersistentDictionary =
                Add(new PersistentDictionary(persistentSource, new ModifiedJTokenComparer(x => x.Value<string>("key"))));
            Documents = new PersistentDictionaryAdapter(txId, documentsPersistentDictionary)
            {
                {"ByKey", documentsPersistentDictionary.AddSecondaryIndex(x => x.Value<string>("key"))},
                {"ById", documentsPersistentDictionary.AddSecondaryIndex(x => x.Value<string>("id"))},
                {"ByEtag", documentsPersistentDictionary.AddSecondaryIndex(x => x.Value<byte[]>("etag"))}
            };

            PersistentDictionary documentsInTransactionPersistentdictionary =
                Add(new PersistentDictionary(persistentSource, new ModifiedJTokenComparer(x => x.Value<string>("key"))));
            DocumentsModifiedByTransactions = new PersistentDictionaryAdapter(txId,
                                                                              documentsInTransactionPersistentdictionary)
            {
                {"ByTxId", documentsInTransactionPersistentdictionary.AddSecondaryIndex(x => x.Value<byte[]>("txId"))}
            };
            Transactions = new PersistentDictionaryAdapter(txId,
                                                           Add(new PersistentDictionary(persistentSource,
                                                                                        new ModifiedJTokenComparer(
                                                                                            x => x.Value<byte[]>("txId")))));

            IndexingStats = new PersistentDictionaryAdapter(txId,
                                                            Add(new PersistentDictionary(persistentSource,
                                                                                         new ModifiedJTokenComparer(
                                                                                             x =>
                                                                                             x.Value<string>("index")))));

            PersistentDictionary mappedResultsPersistentDictioanry = Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance));
            MappedResults = new PersistentDictionaryAdapter(txId, mappedResultsPersistentDictioanry)
            {
                {"ByViewAndReduceKey", mappedResultsPersistentDictioanry.AddSecondaryIndex(x => new JObject
                    {
                        {"view", x.Value<string>("view")},
                        {"reduceKey", x.Value<string>("reduceKey")}
                    })},
               {"ByViewAndDocumentId", mappedResultsPersistentDictioanry.AddSecondaryIndex(x => new JObject
                    {
                        {"view", x.Value<string>("view")},
                        {"docId", x.Value<string>("docId")}
                    })}
            };

            var queuesPersistentDictioanry = Add(new PersistentDictionary(persistentSource, 
                                                                    new ModifiedJTokenComparer(x=> new JObject
                                                                    {
                                                                        {"name", x.Value<string>("name")},
                                                                        {"id", x.Value<byte[]>("id")},
                                                                    })));
            Queues = new PersistentDictionaryAdapter(txId, queuesPersistentDictioanry)
            {
                {"ByName", queuesPersistentDictioanry.AddSecondaryIndex(x=>x.Value<string>("name"))}
            };
        }

        public PersistentDictionaryAdapter  Queues { get; private set; }

        public PersistentDictionaryAdapter MappedResults { get; private set; }

        public PersistentDictionaryAdapter IndexingStats { get; private set; }

        public PersistentDictionaryAdapter Transactions { get; private set; }

        public PersistentDictionaryAdapter DocumentsModifiedByTransactions { get; private set; }

        public PersistentDictionaryAdapter Documents { get; private set; }

        public PersistentDictionaryAdapter Attachments { get; private set; }

        public PersistentDictionaryAdapter Identity { get; private set; }

        public PersistentDictionaryAdapter Details { get; private set; }

        public IDisposable BeginTransaction()
        {
            if (txId.Value != Guid.Empty)
                return new DisposableAction(() => { }); // no op, already in tx

            txId.Value = Guid.NewGuid();

            return new DisposableAction(() =>
            {
                if (txId.Value != Guid.Empty) // tx not committed
                    Rollback();
            });
        }

        public void Commit()
        {
            if (txId.Value == Guid.Empty)
                return;

            Commit(txId.Value);
        }

        public void Rollback()
        {
            Rollback(txId.Value);

            txId.Value = Guid.Empty;
        }
    }
}