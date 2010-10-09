using System;
using System.Threading;
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
        readonly ThreadLocal<Guid> txId = new ThreadLocal<Guid>(() => Guid.Empty);

        public TableStorage(IPersistentSource persistentSource) : base(persistentSource)
        {
            Details = new PersistentDictionaryAdapter(txId, Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance)));
            Identity = new PersistentDictionaryAdapter(txId, Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance)));

            var attachmentPersistentDictionary = Add(new PersistentDictionary(persistentSource, new ModifiedJTokenComparer(x=>x.Value<string>("key"))));
            Attachments = new PersistentDictionaryAdapter(txId, attachmentPersistentDictionary)
            {
                {"ByEtag", attachmentPersistentDictionary.AddSecondaryIndex(x => x.Value<byte[]>("etag"))}
            };
        }

        public PersistentDictionaryAdapter Attachments { get; set; }

        public PersistentDictionaryAdapter Identity { get; private set; }

        public PersistentDictionaryAdapter Details { get; private set; }

        public IDisposable BeginTransaction()
        {
            if (txId.Value != Guid.Empty)
                return new DisposableAction(() => { });// no op, already in tx

            txId.Value = Guid.NewGuid();

            return new DisposableAction(() =>
            {
                if (txId.Value != Guid.Empty)// tx not committed
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