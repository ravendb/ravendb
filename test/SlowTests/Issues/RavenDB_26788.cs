using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26788 : ClusterTestBase
    {
        public RavenDB_26788(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Sharding)]
        public async Task LocalDeletedRangeUpdateShouldSupersedeMigratedDeletedRange()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string id = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);

                    var tsf = session.TimeSeriesFor(id, "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), 59d);
                    tsf.Append(baseline.AddMinutes(2), 69d);
                    tsf.Append(baseline.AddMinutes(3), 79d);
                    session.SaveChanges();
                }

                // a deleted range authored on the original shard
                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor(id, "Heartrate").Delete(baseline.AddMinutes(1), baseline.AddMinutes(2));
                    session.SaveChanges();
                }

                // bucket migration rewrites the deleted-range change vector into the composite 'order|version' shape
                await Sharding.Resharding.MoveShardForId(store, id);

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                var newShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));

                string migratedRangeChangeVector;
                using (newShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    migratedRangeChangeVector = newShard.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(ctx, 0).Single().ChangeVector;
                }

                // precondition: the migrated deleted range carries a composite change vector
                Assert.Contains("|", migratedRangeChangeVector);

                // a local delete that overlaps the migrated range must supersede it, not conflict with it
                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor(id, "Heartrate").Delete(baseline.AddMinutes(1), baseline.AddMinutes(3));
                    session.SaveChanges();
                }

                using (newShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var latestDeletedRange = newShard.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(ctx, 0).OrderByDescending(x => x.Etag).First();
                    var status = ChangeVector.GetConflictStatus(ctx, migratedRangeChangeVector, latestDeletedRange.ChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                }
            }
        }
    }
}
