using System;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19545 : RavenTestBase
{
    public RavenDB_19545(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void RemovingTimeSeriesEntryShouldAffectCache()
    {
        const string docId = "user/1";
        const string timeSeriesName = "HeartRates";
        const string tag = "watches/fitbit";

        using (var store = GetDocumentStore())
        using (var session = store.OpenSession())
        {
            session.Store(new User { Name = "Lev" }, docId);

            var tsf = session.TimeSeriesFor(docId, timeSeriesName);
            tsf.Append(DateTime.Today.AddHours(23), new[] { 67d }, tag);
            session.SaveChanges();

            var entries = session.TimeSeriesFor(docId, timeSeriesName).Get();
            Assert.Equal(1, entries.Length);

            session.TimeSeriesFor(docId, timeSeriesName).Delete(DateTime.MinValue, DateTime.MaxValue);
            session.SaveChanges();

            var entries2 = session.TimeSeriesFor(docId, timeSeriesName).Get();
            Assert.Null(entries2);
        }
    }

    [Fact]
    public void RemovingTimeSeriesEntryShouldAffectCache2()
    {
        const string docId = "user/1";
        const string timeSeriesName = "HeartRates";
        const string tag = "watches/fitbit";
        var baseline = DateTime.UtcNow;

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Lev" }, docId);

                var tsf = session.TimeSeriesFor(docId, timeSeriesName);
                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddDays(i), i, tag);
                    session.SaveChanges();
                }

                var entries = session.TimeSeriesFor(docId, timeSeriesName).Get(baseline.AddDays(9), baseline.AddDays(11));
                Assert.Equal(1, entries.Length);

                entries = session.TimeSeriesFor(docId, timeSeriesName).Get(baseline.AddDays(3), baseline.AddDays(8));
                Assert.Equal(5, entries.Length);

                session.TimeSeriesFor(docId, timeSeriesName).Delete(baseline.AddDays(4), baseline.AddDays(7));
                session.SaveChanges();

                var entries2 = session.TimeSeriesFor(docId, timeSeriesName).Get();
                Assert.Equal(6, entries2.Length);
            }
        }
    }

    [Fact]
    public void RemovingTimeSeriesEntryShouldAffectCache3()
    {
        const string docId = "user/1";
        const string timeSeriesName = "HeartRates";
        const string tag = "watches/fitbit";
        var baseline = DateTime.UtcNow;

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Lev" }, docId);

                var tsf = session.TimeSeriesFor(docId, timeSeriesName);
                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddDays(i), i, tag);
                    session.SaveChanges();
                }

                var entries = session.TimeSeriesFor(docId, timeSeriesName).Get(baseline.AddDays(9).AddSeconds(1), baseline.AddDays(11));
                Assert.Equal(1, entries.Length);

                entries = session.TimeSeriesFor(docId, timeSeriesName).Get(null, baseline.AddDays(8));
                Assert.Equal(8, entries.Length);

                session.TimeSeriesFor(docId, timeSeriesName).Delete(null, baseline.AddDays(7));
                session.SaveChanges();

                var entries2 = session.TimeSeriesFor(docId, timeSeriesName).Get();
                Assert.Equal(3, entries2.Length);
            }
        }
    }

    [Fact]
    public void RemovingTimeSeriesEntryShouldAffectCache4()
    {
        const string docId = "user/1";
        const string timeSeriesName = "HeartRates";
        const string tag = "watches/fitbit";
        var baseline = DateTime.UtcNow;

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Lev" }, docId);

                var tsf = session.TimeSeriesFor(docId, timeSeriesName);
                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddDays(i), i, tag);
                    session.SaveChanges();
                }

                var entries = session.TimeSeriesFor(docId, timeSeriesName).Get(baseline.AddDays(9), baseline.AddDays(11));
                Assert.Equal(1, entries.Length);

                entries = session.TimeSeriesFor(docId, timeSeriesName).Get(baseline.AddDays(1));
                Assert.Equal(9, entries.Length);

                session.TimeSeriesFor(docId, timeSeriesName).Delete(baseline.AddDays(6), null);
                session.SaveChanges();

                var entries2 = session.TimeSeriesFor(docId, timeSeriesName).Get();
                Assert.Equal(5, entries2.Length);
            }
        }
    }

    [Fact]
    public async Task RemovingTimeSeriesEntryShouldAffectCacheAsync()
    {
        const string docId = "user/1";
        const string timeSeriesName = "HeartRates";
        const string tag = "watches/fitbit";

        using (var store = GetDocumentStore())
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Lev" }, docId);

            var tsf = session.TimeSeriesFor(docId, timeSeriesName);
            tsf.Append(DateTime.Today.AddHours(23), new[] { 67d }, tag);
            await session.SaveChangesAsync();

            var entries = await session.TimeSeriesFor(docId, timeSeriesName).GetAsync();
            Assert.Equal(1, entries.Length);

            session.TimeSeriesFor(docId, timeSeriesName).Delete();
            await session.SaveChangesAsync();

            var entries2 = await session.TimeSeriesFor(docId, timeSeriesName).GetAsync();
            Assert.Null(entries2);
        }
    }
}
