using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Util;
using Raven.Server.Documents.ETL;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedEtlTestBase
    {
        internal readonly RavenTestBase _parent;

        public ShardedEtlTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }


        public Task<ManualResetEventSlim> WaitForEtlAsync(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate, int count) => WaitForEtlAsync(store.Database, predicate, count);

        public async Task<ManualResetEventSlim> WaitForEtlAsync(string database, Func<string, EtlProcessStatistics, bool> predicate, int count)
        {
            if (count < 1)
                throw new ArgumentOutOfRangeException(nameof(count));

            var mre = new ManualResetEventSlim();
            var confirmations = new ConcurrentDictionary<int, byte>();
            var size = 0;
            var numberOfShards = 0;

            await foreach (var shard in _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(database))
            {
                numberOfShards++;
                shard.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate?.Invoke($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics) != false)
                    {
                        confirmations.TryAdd(shard.ShardNumber, 0);
                        if (Interlocked.Increment(ref size) == count)
                        {
                            confirmations.Clear();
                            size = 0;
                            mre.Set();
                        }
                    }
                };
            }

            if (numberOfShards < count)
            {
                mre.Set();
                throw new ArgumentOutOfRangeException(nameof(count));
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
            return AsyncHelpers.RunSync(() => WaitForEtlAsync(store, predicate, count: 1));
        }
    }
}
