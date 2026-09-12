using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sharding.Queries
{
    public class RavenDB_MapReduceStreamMergeArenaReset : RavenTestBase
    {
        public RavenDB_MapReduceStreamMergeArenaReset(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Group { get; set; }
            public int Value { get; set; }
            public string Blob { get; set; }
        }

        private class BigReduce : AbstractIndexCreationTask<Item, BigReduce.Result>
        {
            public class Result
            {
                public string Group { get; set; }
                public int Value { get; set; }
                public string Blob { get; set; }
            }

            public BigReduce()
            {
                Map = items => from i in items
                               select new { i.Group, i.Value, i.Blob };

                Reduce = results => from r in results
                                    group r by r.Group into g
                                    select new
                                    {
                                        Group = g.Key,
                                        Value = g.Sum(x => x.Value),
                                        Blob = g.Select(x => x.Blob).FirstOrDefault()
                                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        // Streaming a sharded map-reduce query merges each shard's reduced partials. The shard-stream
        // enumerators build their items into a pooled JSON context whose arena is reset by MoveNext once
        // it crosses ~4MB (StreamOperation.CheckIfContextOrCacheNeedToBeRenewed). By making each reduced
        // group large (a ~2MB blob carried through the reduce), a single streamed partial already pushes a
        // shard's arena over that threshold, so advancing that shard resets its arena. If the merge
        // advances a shard while still holding one of its items for the re-reduce, that item is freed (and
        // overwritten by the next group) before it is read -> dropped/torn results. This asserts the merge
        // survives the reset. Regression for the sharded map-reduce streaming re-reduce.
        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Streaming_MapReduce_Merge_SurvivesShardStreamArenaReset()
        {
            const int groups = 12;
            const int blobSize = 2 * 1024 * 1024; // per-group reduced output exceeds the 4MB arena-reset threshold
            var blob = new string('x', blobSize);

            using var store = Sharding.GetDocumentStore();
            new BigReduce().Execute(store);

            using (var bulk = store.BulkInsert())
            {
                for (int g = 0; g < groups; g++)
                    bulk.Store(new Item { Group = "g" + g.ToString("D2"), Value = 1, Blob = blob }, $"items/{g}");
            }

            Indexes.WaitForIndexing(store);

            var results = new List<BigReduce.Result>();
            using (var session = store.OpenSession())
            using (var stream = session.Advanced.Stream(session.Query<BigReduce.Result, BigReduce>().OrderBy(x => x.Group)))
            {
                while (stream.MoveNext())
                    results.Add(stream.Current.Document);
            }

            Assert.Equal(groups, results.Count);
            foreach (var r in results)
            {
                Assert.Equal(1, r.Value);
                Assert.Equal(blobSize, r.Blob.Length); // a freed/torn blittable would corrupt the carried blob
            }

            Assert.Equal(
                Enumerable.Range(0, groups).Select(g => "g" + g.ToString("D2")).OrderBy(x => x).ToList(),
                results.Select(r => r.Group).OrderBy(x => x).ToList());
        }
    }
}
