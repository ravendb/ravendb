using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents;
using Raven.Server.Documents.ETL;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedEtlTestsBase
    {
        internal readonly RavenTestBase _parent;

        public ShardedEtlTestsBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }


        public ManualResetEventSlim WaitForEtl(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate, int count)
        {
            var dbs = _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();
            if (count < 1 || count > dbs.Count)
                throw new ArgumentOutOfRangeException(nameof(count));

            var mre = new ManualResetEventSlim();
            var confirmations = new ConcurrentDictionary<int, byte>();
            
            foreach (var task in dbs)
            {
                var shard = task.Result;

                shard.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    {
                        confirmations.TryAdd(shard.ShardNumber, 0);
                        if (confirmations.Count == count)
                            mre.Set();
                    }
                };
            }

            return mre;
        }

        public IEnumerable<ManualResetEventSlim> WaitForEtlOnAllShards(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var dbs = _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();
            var list = new List<ManualResetEventSlim>(dbs.Count);
            foreach (var task in dbs)
            {
                var mre = new ManualResetEventSlim();
                list.Add(mre);

                var db = task.Result;
                db.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                        mre.Set();
                };
            }

            return list;
        }


        public ManualResetEventSlim WaitForEtl(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            return WaitForEtl(store, predicate, count: 1);
        }
    }
}
