using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10329 : RavenTestBase
    {
        public RavenDB_10329(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGetFacetsOnFieldsWithNamesThatAreReserevedKeywords()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "john",
                        Group = 1
                    });
                    session.Store(new User
                    {
                        Name = "ringo",
                        Group = 1
                    });
                    session.Store(new User
                    {
                        Name = "george",
                        Group = 2
                    });
                    session.Store(new User
                    {
                        Name = "paul",
                        Group = 2
                    });
                    session.SaveChanges();
                }

                new UsersIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User, UsersIndex>()
                        .AggregateBy(x => x.ByField(nameof(User.Group)))
                        .Execute();

                    Assert.Equal(1, result.Count);

                    Assert.True(result.TryGetValue("Group", out var facetResult));
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal("1", facetResult.Values[0].Range);
                    Assert.Equal(2, facetResult.Values[0].Count);

                    Assert.Equal("2", facetResult.Values[1].Range);
                    Assert.Equal(2, facetResult.Values[1].Count);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public int Group { get; set; }
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Name,
                                  doc.Group
                              };
            }
        }

    }
}
