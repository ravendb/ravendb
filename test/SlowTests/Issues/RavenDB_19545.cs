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
