using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14533 : RavenTestBase
    {
        public RavenDB_14533(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TimeSeriesSegmantsSummary()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2000, 1, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    
                    for (int i = 0; i < 15000; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] { 58d + i }, "fitbit");
                        
                    }

                    session.SaveChanges();
                }

                var sum = 0;
                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var summary = db.DocumentsStorage.TimeSeriesStorage.GetSegmantsSummary(ctx, "users/1", "Heartrate");

                    sum += summary.Sum(seg => seg.numberOfEntries);
                }

                Assert.Equal(15000, sum);

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate").Delete(baseline, baseline.AddMinutes(99));

                    session.SaveChanges();
                }
                sum = 0;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var summary = db.DocumentsStorage.TimeSeriesStorage.GetSegmantsSummary(ctx, "users/1", "Heartrate");

                    sum += summary.Sum(seg => seg.numberOfLiveEntries);
                }
                Assert.Equal(14900, sum);
            }
        }
    }
}
