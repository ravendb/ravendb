using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_23579 : RavenTestBase
    {
        public RavenDB_23579(ITestOutputHelper output) : base(output)
        {
        }

        const string script = """
                              loadToUsers(this);
                              function loadTimeSeriesOfUsersBehavior(doc, ts)
                              {
                                  if (ts.startsWith("INC")){
                                      return false;
                                  }
                                  return true;
                              }
                              """;

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Counters)]
        public async Task TombstoneAfterCounterDeletionWithEtl()
        {
            var (src, _, _) = Etl.CreateSrcDestAndAddEtl("Users", script);

            var user = new User();
            var order = new Order();
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(order);
                session.CountersFor(order.Id).Increment("AA", 3);
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            using (var session = src.OpenAsyncSession())
            {
                session.CountersFor(order.Id).Delete("AA");
                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(src);
            var tombstoneCleaner = database.TombstoneCleaner;
            var deletedTombstonesCount = await tombstoneCleaner.ExecuteCleanup();
            Assert.Equal(2, deletedTombstonesCount); //This should be changed to 1 after issue RavenDB-23874 is fixed.
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.TimeSeries)]
        public async Task TombstoneAfterTsDeletionWithEtl()
        {
            var (src, _, _) = Etl.CreateSrcDestAndAddEtl("Users", script);

            var user = new User();
            var order = new Order();
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(order);
                session.TimeSeriesFor<FilteredReplicationTests.HeartRateMeasure>(order.Id).Append(DateTime.UtcNow, new FilteredReplicationTests.HeartRateMeasure
                {
                    HeartRate = 34
                });
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            using (var session = src.OpenAsyncSession())
            {
                session.TimeSeriesFor<FilteredReplicationTests.HeartRateMeasure>(order.Id).Delete();
                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(src);
            var tombstoneCleaner = database.TombstoneCleaner;
            var deletedTombstonesCount = await tombstoneCleaner.ExecuteCleanup();
            Assert.Equal(1, deletedTombstonesCount);
        }
    }
}
