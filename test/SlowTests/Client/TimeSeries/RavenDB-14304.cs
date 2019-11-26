using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
{
    public class RavenDB_14304 : RavenTestBase
    {
        public RavenDB_14304(ITestOutputHelper output) : base(output)
        {
        }

        [Fact (Skip = "RavenDB-14304")]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, "fitbit", new[] { 60d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should go to a new segment
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1), "fitbit", new[] { 60d, 70 });

                    session.SaveChanges();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var reader = db.DocumentsStorage.TimeSeriesStorage.GetReader(ctx, "users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue);

                    var segmentsOrValues = reader.SegmentsOrValues().ToList();

                    Assert.Equal(2, segmentsOrValues.Count);
                    Assert.True(segmentsOrValues[0].Segment.Start <= segmentsOrValues[0].Segment.End);
                    Assert.True(segmentsOrValues[1].Segment.Start <= segmentsOrValues[1].Segment.End);

                }
            }
        }

    }
}
