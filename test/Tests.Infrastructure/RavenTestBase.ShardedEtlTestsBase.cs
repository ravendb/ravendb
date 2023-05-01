using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;

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


        public ManualResetEventSlim WaitForEtl(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate, int count = 1)
        {
            var dbs = _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();
            if (count < 1 || count > dbs.Count)
                throw new ArgumentOutOfRangeException(nameof(count));

            var mre = new ManualResetEventSlim();
            var confirmations = new HashSet<int>();

            foreach (var task in dbs)
            {
                var shard = task.Result;

                shard.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    {
                        confirmations.Add(shard.ShardNumber);
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


    }
}
