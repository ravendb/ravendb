using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Bugs.Zhang
{
    public class UseMaxForLongTypeInReduce : RavenTestBase
    {
        public UseMaxForLongTypeInReduce(ITestOutputHelper output) : base(output)
        {
        }

        private const string Map = @"
from doc in docs.Items
from tag in doc.Tags
select new { Name = tag.Name, CreatedTimeTicks = doc.CreatedTimeTicks }
";

        private const string Reduce = @"
from agg in results
group agg by agg.Name into g
let createdTimeTicks = g.Max(x => (long)x.CreatedTimeTicks)
select new {Name = g.Key, CreatedTimeTicks = createdTimeTicks}
";

        private class Item
        {
            public string Id { get; set; }

            public string Topic { get; set; }

            public Tag[] Tags { get; set; }

            public long CreatedTimeTicks { get; set; }
        }

        private class Tag
        {
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseMax(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Topic = "RavenDB is Hot", CreatedTimeTicks = SystemTime.UtcNow.Ticks, Tags = new[] { new Tag { Name = "DB" }, new Tag { Name = "NoSQL" } } });

                    session.Store(new Item { Topic = "RavenDB is Fast", CreatedTimeTicks = SystemTime.UtcNow.AddMinutes(10).Ticks, Tags = new[] { new Tag { Name = "NoSQL" } } });

                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name = "test",
                        Maps = { Map },
                        Reduce = Reduce,
                    }}));

                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<object>("test").WaitForNonStaleResults().ToArray();
                }

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
