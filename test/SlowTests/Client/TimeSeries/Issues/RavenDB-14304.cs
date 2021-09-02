using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14304 : RavenTestBase
    {
        public RavenDB_14304(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline, new[] { 60d }, "fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should go to a new segment
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMonths(1), new[] { 60d, 70 }, "fitbit");

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

        [Fact]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline, new[] { 60d }, "fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should go to a new segment
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMonths(1), new[] { 60d, 70 }, "fitbit");
                    tsf.Append(baseline.AddMonths(2), new[] { 60d, 70,80 }, "fitbit");

                    session.SaveChanges();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var reader = db.DocumentsStorage.TimeSeriesStorage.GetReader(ctx, "users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue);

                    var segmentsOrValues = reader.SegmentsOrValues().ToList();

                    Assert.Equal(3, segmentsOrValues.Count);
                    Assert.True(segmentsOrValues[0].Segment.Start <= segmentsOrValues[0].Segment.End);
                    Assert.True(segmentsOrValues[1].Segment.Start <= segmentsOrValues[1].Segment.End);

                }
            }
        }

        [Fact]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment3()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline, new[] { 60d }, "fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should go to a new segment
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(1), new[] { 60d }, "fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMonths(1), new[] { 60d, 70 }, "fitbit");

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

        [Fact]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment4()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(10), new[] { 10d }, "fitbit");

                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(20), new[] { 10d }, "fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddSeconds(11), new[] { 10d }, "fitbit");

                    tsf.Append(baseline.AddSeconds(12), new[] { 60d, 70 }, "fitbit");


                    session.SaveChanges();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var reader = db.DocumentsStorage.TimeSeriesStorage.GetReader(ctx, "users/ayende", "Heartrate", DateTime.MinValue, DateTime.MaxValue);

                    var segmentsOrValues = reader.SegmentsOrValues().ToList();

                    Assert.Equal(1, segmentsOrValues.Count);
                    Assert.True(segmentsOrValues[0].Segment.Start <= segmentsOrValues[0].Segment.End);
                    Assert.Equal(4, reader.AllValues().Count());
                }
            }
        }

    }
}
