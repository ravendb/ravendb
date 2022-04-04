using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues;

public class RavenDB_16365: RavenTestBase
{
    private record MyDocument(string Id);

    public RavenDB_16365(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Read_NameTimeSeries_Back()
    {
        var user = new User {Name = "RavenDB"};
        const string userId = "users/1";
        using (var documentStore = GetDocumentStore())
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                await session.StoreAsync(user, userId);
                var ts = session.TimeSeriesFor<StoreMetrics>(user);
                ts.Append(DateTime.Now, new StoreMetrics());
                await session.SaveChangesAsync();
                var exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await ts.GetAsync(); 
                });
                Assert.Contains("Properties are not allowed when using Struct. Use Fields instead.", exception.Message);
            }
        }
    }

    private struct StoreMetrics
    {
        [TimeSeriesValue(0)]
        public double Downloads { get; set; }
    
        [TimeSeriesValue(1)]
        public double ReDownloads { get; set; }
    
        [TimeSeriesValue(2)]
        public double Uninstalls { get; set; }
    
        [TimeSeriesValue(3)]
        public double Updates { get; set; }
    
        [TimeSeriesValue(4)]
        public double Returns { get; set; }
    }
}

