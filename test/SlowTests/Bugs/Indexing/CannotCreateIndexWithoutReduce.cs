using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class QueryWithStaticIndexesAndCommonBaseClass : RavenTestBase
    {
        public QueryWithStaticIndexesAndCommonBaseClass(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCreateCorrectIndexForNestedObjectWithReferenceId(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Roots_ByUserId().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Root
                    {
                        User = new UserReference
                        {
                            Id = "Users/1"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entities = session.Query<Root, Roots_ByUserId>()
                        .Customize(x => x.WaitForNonStaleResults());
                    Assert.Equal(1, entities.Count());
                }
            }
        }

        private class Roots_ByUserId : AbstractIndexCreationTask<Root>
        {
            public Roots_ByUserId()
            {
                Map = cases => from e in cases
                               select new
                               {
                                   User_Id = e.User.Id
                               };
            }
        }

        private class Root : Identifiable
        {
            public UserReference User { get; set; }
        }

        private class UserReference : Identifiable
        {
        }

        private class Identifiable
        {
            public string Id { get; set; }
        }
    }
}
