using System;
using System.Diagnostics;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Munin;

namespace Raven.Storage.Managed.Impl
{
    public class TableStorage : Munin.Database
    {

        public TableStorage(IPersistentSource persistentSource)
            : base(persistentSource)
        {
            Details = new PersistentDictionaryAdapter(CurrentTransactionId,
                                                     Add(new Table(JTokenComparer.Instance)
                                                     {
                                                         Name = "Details"
                                                     }));

            Identity = new PersistentDictionaryAdapter(CurrentTransactionId,
                                                       Add(new Table(new ModifiedJTokenComparer(x=>x.Value<string>("name")))
                                                       {
                                                           Name = "Identity"
                                                       }));

            Attachments = new PersistentDictionaryAdapter(CurrentTransactionId, Add(new Table(new ModifiedJTokenComparer(x => x.Value<string>("key")))
            {
                Name = "Attachments"
            }))
            {
                {"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))}
            };

            Documents = new PersistentDictionaryAdapter(CurrentTransactionId, Add(new Table(new ModifiedJTokenComparer(x => x.Value<string>("key")))
            {
                Name = "Documents"
            }))
            {
                {"ByKey", x => x.Value<string>("key")},
                {"ById", x => x.Value<string>("id")},
                {"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))}
            };

            DocumentsModifiedByTransactions = new PersistentDictionaryAdapter(CurrentTransactionId, 
                Add(new Table(new ModifiedJTokenComparer(x => new JObject
                {
                    {"key", x.Value<string>("key")},
                }))
                {
                    Name = "DocumentsModifiedByTransactions"
                }))
            {
                {"ByTxId", x => new ComparableByteArray(x.Value<byte[]>("txId"))}
            };
            Transactions = new PersistentDictionaryAdapter(CurrentTransactionId,
                Add(new Table(new ModifiedJTokenComparer(x => x.Value<byte[]>("txId")))
                {
                    Name = "Transactions"
                }));

            IndexingStats = new PersistentDictionaryAdapter(CurrentTransactionId,
                Add(new Table(new ModifiedJTokenComparer(x =>x.Value<string>("index")))
                {
                    Name = "IndexingStats"
                }));

            MappedResults = new PersistentDictionaryAdapter(CurrentTransactionId,
                                                            Add(new Table(JTokenComparer.Instance)
                                                            {
                                                                Name = "MappedResults"
                                                            }))
            {
                {"ByViewAndReduceKey", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("reduceKey"))},
                {"ByViewAndDocumentId", x => Tuple.Create(x.Value<string>("view"), x.Value<string>("docId"))}
            };

            Queues = new PersistentDictionaryAdapter(CurrentTransactionId, Add(new Table(new ModifiedJTokenComparer(x=> new JObject
                                                                                        {
                                                                                            {"name", x.Value<string>("name")},
                                                                                            {"id", x.Value<byte[]>("id")},
                                                                                        }))
            {
                Name = "Queues"
            }))
            {
                {"ByName", x=>x.Value<string>("name")}
            };

            Tasks = new PersistentDictionaryAdapter(CurrentTransactionId, Add(new Table(new ModifiedJTokenComparer(x => new JObject
                                                                                        {
                                                                                            {"index", x.Value<string>("index")},
                                                                                            {"id", x.Value<byte[]>("id")},
                                                                                        }))
            {
                Name = "Tasks"
            }))
            {
                {"ByIndexAndTime", x=>Tuple.Create(x.Value<string>("index"), x.Value<DateTime>("time")) },
                {"ByIndexAndType", x=>Tuple.Create(x.Value<string>("index"), x.Value<string>("type")) }
            };
        }

        public PersistentDictionaryAdapter Details { get; private set; }

        public PersistentDictionaryAdapter Tasks { get; private set; }

        public PersistentDictionaryAdapter  Queues { get; private set; }

        public PersistentDictionaryAdapter MappedResults { get; private set; }

        public PersistentDictionaryAdapter IndexingStats { get; private set; }

        public PersistentDictionaryAdapter Transactions { get; private set; }

        public PersistentDictionaryAdapter DocumentsModifiedByTransactions { get; private set; }

        public PersistentDictionaryAdapter Documents { get; private set; }

        public PersistentDictionaryAdapter Attachments { get; private set; }

        public PersistentDictionaryAdapter Identity { get; private set; }

       
    }
}