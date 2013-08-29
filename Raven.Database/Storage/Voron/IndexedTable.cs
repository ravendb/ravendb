using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Voron;
using Voron.Impl;

namespace Raven.Database.Storage.Voron
{
    public class IndexedTable : Table
    {
        private readonly ConcurrentDictionary<string, Table> tableIndexes;
        private readonly object indexSyncObject;

        public IndexedTable(string treeName,params string[] indexNames)
            : base(treeName)
        {
            if(indexNames == null)
                throw new ArgumentNullException("indexNames");

            tableIndexes = new ConcurrentDictionary<string, Table>();
            indexSyncObject = new object();
            
            foreach (var indexName in indexNames)
                GetIndex(indexName);
        }

        public Table GetIndex(string indexName)
        {
            if (String.IsNullOrWhiteSpace(indexName))
            {
                throw new ArgumentNullException(indexName);
            }

            lock (indexSyncObject)
            {
                var indexKey = GetIndexKey(indexName);

                var relevantIndexTable = tableIndexes.GetOrAdd(indexKey, (indexTreeName) => new Table(indexTreeName));
                return relevantIndexTable;
            }
        }

        public string GetIndexKey(string indexName)
        {
            return treeName + "_" + indexName;
        }
    }
}
