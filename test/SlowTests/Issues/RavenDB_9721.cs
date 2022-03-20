using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9721 : RavenTestBase
    {
        public RavenDB_9721(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_include_last_tombstone_as_cutoff_etag()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Arek"
                    }, "users/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();

                    QueryStatistics stats = null;

                    Assert.Throws<TimeoutException>(() => session.Query<User, Users_ByName>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(1)))
                        .Statistics(out stats)
                        .AggregateBy(x => x.ByField(y => y.Name)).Execute());

                    Assert.True(stats.IsStale);

                    Assert.Throws<TimeoutException>(() => session.Query<User, Users_ByName>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(1)))
                        .Statistics(out stats)
                        .SuggestUsing(x => x.ByField(y => y.Name, "arek").WithOptions(new SuggestionOptions
                        {
                            PageSize = 10
                        }))
                        .Execute());

                    Assert.True(stats.IsStale);
                }
            }
        }

        public class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users
                    select new
                    {
                        u.Name
                    };
                Suggestion(x => x.Name);
            }
        }
    }
}
