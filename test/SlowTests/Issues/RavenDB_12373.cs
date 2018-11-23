using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12373 : RavenTestBase
    {
        [Fact]
        public void StringJoin()
        {
            var store = GetDocumentStore();
            store.ExecuteIndex(new TestIndex());

            using (var session = store.OpenSession())
            {
                session.Store(new Entity { Prop = "a" });
                session.Store(new Entity { Prop = "a" });
                session.Store(new Entity { Prop = "b" });
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();

                WaitForIndexing(store);

                var results = session.Query<Entity, TestIndex>().ToList();

                Assert.Equal(3, results.Count);
            }
        }

        private class Entity
        {
            public string Prop { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<Entity, TestIndex.Result>
        {
            public class Result
            {
                public string Type { get; set; }
            }

            public TestIndex()
            {
                Map = entities => from entity in entities
                    let array = new string[] { "one", "two" }
                    let ones = array.Where(x => x == "one")
                    select new
                    {
                        Type = entity.Prop
                    };
            }
        }
    }
}
