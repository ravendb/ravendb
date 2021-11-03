using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17282 : RavenTestBase
    {
        public RavenDB_17282(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AutoMapIndexFieldsShouldInludeDocumentIdIfItIsTheOnlyField()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeBeforeDeletionOfSupersededAutoIndex)] = "0";
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle)] = "0";
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "joe" }, "users/joe");
                    session.Store(new User { Name = "doe" }, "users/doe");

                    session.SaveChanges();

                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out var stats).Where(x => x.Id.EndsWith("joe")).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Auto/Users/ById()", stats.IndexName);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out stats).OrderBy(x => x.Id).ToList();

                    Assert.Equal("users/doe", users[0].Id);
                    Assert.Equal("users/joe", users[1].Id);
                    Assert.Equal("Auto/Users/ById()", stats.IndexName);

                    var def = store.Maintenance.Send(new GetIndexOperation(stats.IndexName));

                    Assert.True(def.Fields.ContainsKey(Constants.Documents.Indexing.Fields.DocumentIdFieldName));
                    Assert.True(def.Maps.First().Contains(Constants.Documents.Indexing.Fields.DocumentIdFieldName));

                    // extend index name
                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out stats).Where(x => x.Name == "joe").ToList();


                    Assert.Equal(1, users.Count);
                    Assert.Equal("Auto/Users/ByName", stats.IndexName);

                    WaitForUserToContinueTheTest(store);

                    // after extending the index def, the id() fields should not be included
                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out stats).Where(x => x.Id.EndsWith("joe")).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Auto/Users/ByName", stats.IndexName);
                }
            }
        }
    }
}
