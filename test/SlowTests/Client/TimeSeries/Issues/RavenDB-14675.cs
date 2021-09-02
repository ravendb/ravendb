using System.Threading.Tasks;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14675 : RavenTestBase
    {
        public RavenDB_14675(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TimeSeriesNamesInMetadataShouldKeepOriginalCasing()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "companies/1-A");

                    session.TimeSeriesFor("companies/1-A", "Temperature")
                        .Append(baseline.AddMinutes(10), new[] { 17.5d });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");

                    var tsNames = session.Advanced.GetTimeSeriesFor(company);

                    Assert.Equal(1, tsNames.Count);
                    Assert.Equal("Temperature", tsNames[0]); 
                }

                using (var session = store.OpenAsyncSession())
                {
                    // modify the document

                    var company = await session.LoadAsync<Company>("companies/1-A");

                    company.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");
                    var tsNames = session.Advanced.GetTimeSeriesFor(company);

                    Assert.Equal(1, tsNames.Count);
                    Assert.Equal("Temperature", tsNames[0]);
                }
            }
        }

        [Fact]
        public async Task TimeSeriesNamesInMetadataShouldKeepOriginalCasing2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "companies/1-A");

                    session.TimeSeriesFor("companies/1-A", "Temperature")
                        .Append(baseline.AddMinutes(10), 17.5);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");

                    var tsNames = session.Advanced.GetTimeSeriesFor(company);

                    Assert.Equal(1, tsNames.Count);
                    Assert.Equal("Temperature", tsNames[0]);
                }

                using (var session = store.OpenAsyncSession())
                {
                    // modify the document

                    session.TimeSeriesFor("companies/1-A", "HeartRate")
                        .Append(baseline.AddHours(1), new[] { 67.2d });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");
                    var tsNames = session.Advanced.GetTimeSeriesFor(company);

                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("HeartRate", tsNames[0]);
                    Assert.Equal("Temperature", tsNames[1]);
                }
            }
        }

    }
}
