using FastTests;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11885 : RavenTestBase
    {
        public RavenDB_11885(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DocumentStorageReadLastEtagShouldTakeIntoAccountTheLastCounterEtag()
        {
            using (var store = GetDocumentStore(new Options
            {
                DeleteDatabaseOnDispose = true,
                Path = NewDataPath()
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("likes");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("downloads");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("likes");
                    session.SaveChanges();
                }

                var toggleResult = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
                Assert.True(toggleResult.Disabled);
                Assert.True(toggleResult.Success);

                toggleResult = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
                Assert.False(toggleResult.Disabled);
                Assert.True(toggleResult.Success);

                using (var session = store.OpenSession())
                {
                    // this used to fail with exception:
                    // "Voron.Exceptions.VoronErrorException: Attempt to add duplicate value 8
                    // to AllCountersEtags on Collection.Counters.users"

                    // check that it works fine now

                    session.CountersFor("users/1").Increment("downloads");
                    session.SaveChanges();
                }
            }
        }
    }
}
