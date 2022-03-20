using System.Linq;
using System.Text.RegularExpressions;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_326 : RavenTestBase
    {
        public RDBC_326(ITestOutputHelper output) : base(output)
        {
        }

        private class Entity
        {
            public string Id { get; set; }
            public string Value { get; set; }
        }

        private class EntityIndex : AbstractIndexCreationTask<Entity, EntityIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }

                public bool ContainsFoo { get; set; }
            }

            public EntityIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Id = doc.Id,
                                  ContainsFoo = Regex.IsMatch(doc.Value, "foo", RegexOptions.IgnoreCase)
                              };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanUseRegexInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new EntityIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity
                    {
                        Value = "foo"
                    });

                    session.Store(new Entity
                    {
                        Value = "goo"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<EntityIndex.Result, EntityIndex>()
                        .Where(x => x.ContainsFoo == false)
                        .OfType<Entity>()
                        .Single();

                    Assert.Equal("goo", result.Value);

                    result = session.Query<EntityIndex.Result, EntityIndex>()
                        .Where(x => x.ContainsFoo)
                        .OfType<Entity>()
                        .Single();

                    Assert.Equal("foo", result.Value);
                }
            }
        }
    }
}
