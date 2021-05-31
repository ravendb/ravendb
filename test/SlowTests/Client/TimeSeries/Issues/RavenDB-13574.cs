using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_13574 : RavenTestBase
    {
        public RavenDB_13574(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldHaveTimeSeriesFlagInMetadata()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 58d }, "fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.Contains(nameof(DocumentFlags.HasTimeSeries), flags);
                }
            }
        }

        [Fact]
        public void TimeSeriesFlagShouldBeRemovedFromMetadata()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 58d }, "fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.Contains(nameof(DocumentFlags.HasTimeSeries), flags);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Delete(DateTime.MinValue, DateTime.MaxValue);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.DoesNotContain(nameof(DocumentFlags.HasTimeSeries), flags);
                }
            }
        }

        [Fact]
        public void TimeSeriesNameShouldBeRemovedFromMetadata()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 58d }, "fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate2")
                        .Append(baseline.AddMinutes(1), new[] { 58d }, "apple");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.Contains(nameof(DocumentFlags.HasTimeSeries), flags);

                    var tsNames = session.Advanced.GetTimeSeriesFor(user);

                    Assert.Equal(2, tsNames.Count);
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Heartrate2", tsNames[1]);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate2")
                        .Delete(DateTime.MinValue, DateTime.MaxValue);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.Contains(nameof(DocumentFlags.HasTimeSeries), flags);

                    var tsNames = session.Advanced.GetTimeSeriesFor(user);

                    Assert.Equal(1, tsNames.Count);
                    Assert.Equal("Heartrate", tsNames[0]);
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Delete(DateTime.MinValue, DateTime.MaxValue);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.DoesNotContain(nameof(DocumentFlags.HasTimeSeries), flags);

                    var tsNames = session.Advanced.GetTimeSeriesFor(user);

                    Assert.Empty(tsNames);
                }
            }
        }

        [Fact]
        public async Task CanGetSeriesMinMaxAndCount()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2000, 1, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline, new[] { 60d }, "fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(1), new[] { 62d }, "fitbit");
                    session.SaveChanges();
                }

                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende", "Heartrate")
                                .Append(baseline.AddYears(i).AddMinutes(j), new[] { 58d + i }, "fitbit");
                        }

                        session.SaveChanges();
                    }
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var summary = db.DocumentsStorage.TimeSeriesStorage.GetSeriesSummary(ctx, "users/ayende", "Heartrate");

                    Assert.Equal(10002, summary.Count);
                    Assert.Equal(59, summary.Min[0]);
                    Assert.Equal(68, summary.Max[0]);

                }
            }
        }

        [Fact]
        public async Task CanGetSeriesMinMaxAndCount_MultipleValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2000, 1, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 60d, 180d }, "fitbit");
                    tsf.Append(baseline.AddSeconds(1), new[] { 62d, 178d }, "fitbit");
                    session.SaveChanges();
                }

                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende", "Heartrate")
                                .Append(baseline.AddYears(i).AddMinutes(j), new[] { 58d + i, 170d - i }, "fitbit");
                        }

                        session.SaveChanges();
                    }
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var summary = db.DocumentsStorage.TimeSeriesStorage.GetSeriesSummary(ctx, "users/ayende", "Heartrate");

                    Assert.Equal(10002, summary.Count);

                    Assert.Equal(59, summary.Min[0]);
                    Assert.Equal(68, summary.Max[0]);

                    Assert.Equal(160, summary.Min[1]);
                    Assert.Equal(180, summary.Max[1]);

                }
            }
        }

        [Fact]
        public async Task CanGetSeriesMinMaxAndCount_DifferentNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2000, 1, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline, new[] { 60d }, "fitbit");
                    tsf.Append(baseline.AddSeconds(1), new[] { 62d, 178d, 278 }, "fitbit");
                    session.SaveChanges();
                }

                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende", "Heartrate")
                                .Append(baseline.AddYears(i).AddMinutes(j), new[] { 58d + i, 170d - i }, "fitbit");
                        }

                        session.SaveChanges();
                    }
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var summary = db.DocumentsStorage.TimeSeriesStorage.GetSeriesSummary(ctx, "users/ayende", "Heartrate");

                    Assert.Equal(10002, summary.Count);

                    Assert.Equal(59, summary.Min[0]);
                    Assert.Equal(68, summary.Max[0]);

                    Assert.Equal(160, summary.Min[1]);
                    Assert.Equal(178, summary.Max[1]);

                    Assert.Equal(278, summary.Min[2]);
                    Assert.Equal(278, summary.Max[2]);

                }
            }
        }
    }
}
