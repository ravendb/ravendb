using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class QueryingOverTags : RavenTestBase
    {
        public QueryingOverTags(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_chain_wheres_when_querying_collection_with_any(Options options)
        {
            var entity = new EntityWithTags
            {
                Tags = new[] { "FOO", "BAR" }
            };
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    IQueryable<EntityWithTags> query = session.Query<EntityWithTags>()
                        .Customize(x => x.WaitForNonStaleResults());

                    foreach (var tag in new string[] { "FOO", "BAR" })
                    {
                        string localTag = tag;
                        query = query.Where(e => e.Tags.Any(t => t == localTag));
                    }

                    Assert.NotEmpty(query.ToList());
                }
            }
        }

        private class EntityWithTags
        {
            public System.Collections.Generic.IEnumerable<string> Tags
            {
                get;
                set;
            }
        }
    }
}
