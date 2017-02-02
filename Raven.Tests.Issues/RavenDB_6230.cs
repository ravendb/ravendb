using System;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6230 : RavenTest
    {
        [Fact]
        public void InvalidDynamicIndex()
        {
            using (var store = NewDocumentStore(databaseName: "Test"))
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        bulk.Store(new Entity { Name = $"bar{i}" });
                    }
                }

                using (var session = store.OpenSession())
                {
                    var queryById = session.Query<Entity>()
                        .Where(x => x.Id.In(11, 26, 42, 63, 66))
                        .ToList();

                    Assert.Equal(queryById.Count, 5);

                    var queryByFoo = session.Advanced
                        .DocumentQuery<Entity>()
                        .WhereEquals("FooField", "5")
                        .ToList();

                    queryById = session.Query<Entity>()
                        .Where(x => x.Id.In(11, 26, 42, 63, 66))
                        .ToList();

                    Assert.Equal(queryById.Count, 5);

                }
            }
        }

        private class Entity
        {
            public long Id { get; set; }
            public string Name { get; set; }
            public long? ArticleTemplateId { get; set; }
        }
    }
}
