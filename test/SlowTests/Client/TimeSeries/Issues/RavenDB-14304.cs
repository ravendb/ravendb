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

        [Fact]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment2()
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
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(2), "fitbit", new[] { 60d, 70,80 });

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
                        .Append("Heartrate", baseline.AddSeconds(1), "fitbit", new[] { 60d });
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

        [Fact]
        public async Task AppendingItemThatIsTooFarFromBaselineShouldGoToNewSegment4()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(10), "fitbit", new[] { 10d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(20), "fitbit", new[] { 10d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(11), "fitbit", new[] { 10d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(12), "fitbit", new[] { 60d, 70 });


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
                    Assert.Equal(4, segmentsOrValues[0].Segment.Values.Count());
                }
            }
        }

    }
}
