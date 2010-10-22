using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class PersistentDictionaryAdapter : IEnumerable<Table.ReadResult>
    {
        private readonly ThreadLocal<Guid> txId;
        private readonly Table table;
        private readonly Dictionary<string, SecondaryIndex> secondaryIndices = new Dictionary<string, SecondaryIndex>();

        public override string ToString()
        {
            return table.Name + " (" + Count +")";
        }

        public PersistentDictionaryAdapter(ThreadLocal<Guid> txId, Table table)
        {
            this.txId = txId;
            this.table = table;
        }

        public int Count
        {
            get { return table.ItemsCount; }
        }

        public IEnumerable<JToken> Keys
        {
            get { return table.Keys; }
        }

        public bool Put(JToken key, byte[] value)
        {
            return table.Put(key, value, txId.Value);
        }

        public Table.ReadResult Read(JToken key)
        {
            return table.Read(key, txId.Value);
        }

        public bool Remove(JToken key)
        {
            return table.Remove(key, txId.Value);
        }

        public void Add(string name, Expression<Func<JToken, IComparable>> func)
        {
            secondaryIndices[name] = table.AddSecondaryIndex(func); 
        }

        public IEnumerator<Table.ReadResult> GetEnumerator()
        {
            return table.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public SecondaryIndex this[string indexName]
        {
            get { return secondaryIndices[indexName]; }
        }

        public bool UpdateKey(JToken key)
        {
            return table.UpdateKey(key, txId.Value);
        }
    }
}