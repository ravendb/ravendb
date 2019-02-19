using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Tests.Bugs.Zhang
{
    public class UseMaxForDateTimeTypeInReduce : RavenTestBase
    {
        private const string Map = @"
from doc in docs.Items
from tag in doc.Tags
select new { Name = tag.Name, CreatedTime = doc.CreatedTime.Ticks }
";

        private const string Reduce = @"
from agg in results
group agg by agg.Name into g
let createdTime = g.Max(x => (long)x.CreatedTime)
select new {Name = g.Key, CreatedTime = createdTime}
";

        private class Item
        {
            public string Id { get; set; }

            public string Topic { get; set; }

            public DateTime CreatedTime { get; set; }

            public Tag[] Tags { get; set; }
        }

        private class Tag
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanUseMax()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Topic = "RavenDB is Hot", CreatedTime = SystemTime.UtcNow, Tags = new[] { new Tag { Name = "DB" }, new Tag { Name = "NoSQL" } } });

                    session.Store(new Item { Topic = "RavenDB is Fast", CreatedTime = SystemTime.UtcNow.AddMinutes(10), Tags = new[] { new Tag { Name = "NoSQL" } } });

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
                    session.Advanced.DocumentQuery<object>("test").WaitForNonStaleResults().ToArray<object>();
                }

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
