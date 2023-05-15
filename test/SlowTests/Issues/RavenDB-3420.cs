using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3420 : RavenTestBase
    {
        public RavenDB_3420(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BulkInsert | RavenTestCategory.Sharding)]
        public void bulk_insert_sharded()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.Initialize();

                var entity1 = new Profile { Id = "bulk1", Name = "Hila", Location = "Shard1" };
                var entity2 = new Profile { Name = "Jay", Location = "Shard2" };
                var entity3 = new Profile { Name = "Jay", Location = "Shard1" };
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(entity1, "Shard1/bulk1");
                    bulkInsert.Store(entity2, "Shard2/profiles/1");
                    bulkInsert.Store(entity3, "Shard1/profiles/2");
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Load<Profile>("Shard1/bulk1");
                    Assert.Equal("Shard1", docs.Location);
                    var docs2 = session.Load<Profile>("Shard1/profiles/2");
                    Assert.Equal("Shard1", docs2.Location);

                    var totalDocs = session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(3, totalDocs.Count);
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Load<Profile>("Shard2/profiles/1");
                    Assert.Equal("Shard2", docs.Location);

                    var totalDocs = session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(3, totalDocs.Count);
                }

            }
        }

        private class Profile
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Location { get; set; }
        }
    }
}
