using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
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
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "fitbit", new[] { 58d });
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
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "fitbit", new[] { 58d });
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
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", DateTime.MinValue, DateTime.MaxValue);
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
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "fitbit", new[] { 58d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate2", baseline.AddMinutes(1), "apple", new[] { 58d });
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
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate2", DateTime.MinValue, DateTime.MaxValue);
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
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", DateTime.MinValue, DateTime.MaxValue);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.TryGetValue(Constants.Documents.Metadata.Flags, out var flags);

                    Assert.DoesNotContain(nameof(DocumentFlags.HasTimeSeries), flags);

                    var tsNames = session.Advanced.GetTimeSeriesFor(user);

                    Assert.Null(tsNames);
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
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, "fitbit", new[] { 60d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(1), "fitbit", new[] { 62d });
                    session.SaveChanges();
                }

                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddYears(i).AddMinutes(j), "fitbit", new[] { 58d + i });
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
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, "fitbit", new[] { 60d, 180d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(1), "fitbit", new[] { 62d, 178d });
                    session.SaveChanges();
                }

                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddYears(i).AddMinutes(j), "fitbit", new[] { 58d + i, 170d - i });
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
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, "fitbit", new[] { 60d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddSeconds(1), "fitbit", new[] { 62d, 178d, 278 });
                    session.SaveChanges();
                }

                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddYears(i).AddMinutes(j), "fitbit", new[] { 58d + i, 170d - i });
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
