using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2911 : RavenTestBase
    {
        public RavenDB_2911(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class Index_with_errors : AbstractIndexCreationTask<User>

        {
            public Index_with_errors()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name,
                                   Foo = 1 / (u.Name.Length - u.Name.Length)
                               };
            }
        }


        [Fact]
        public void Dynamic_query_should_not_use_index_with_errors()
        {
            using (var store = GetDocumentStore())
            {
                new Index_with_errors().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 101; i++) //if less than 100 - not enough attempts to determine if the index is problematic
                        session.Store(new User { Name = "Foobar" + i });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors:true);

                Assert.True(SpinWait.SpinUntil(() =>
                {
                    var indexWithErrors = store.Maintenance.Send(new GetIndexStatisticsOperation("Index/with/errors"));
                    return indexWithErrors.IsInvalidIndex;
                }, TimeSpan.FromSeconds(10))); //precaution

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "AB")
                        .ToList();

                    Assert.NotEqual("Index/with/errors", stats.IndexName);
                }
            }
        }
    }
}
