using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB14590 : RavenTestBase
    {
        public RavenDB14590(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetUpdatesTimeSeriesValuesUsingInclude()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new { }, "users/1");
                await s.SaveChangesAsync();
            }

            var baseline = RavenTestHelper.UtcToday;

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();

                var doc = await s.LoadAsync<object>("users/1", 
                    include => include.IncludeTimeSeries("speed", DateTime.MinValue, DateTime.MaxValue));

                var numOfRequests = s.Advanced.NumberOfRequests;

                // should not go to server
                var series = (await s.TimeSeriesFor(doc, "speed")
                    .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                    .ToList();
                
                Assert.Equal(numOfRequests, s.Advanced.NumberOfRequests);
                Assert.Equal(i, series.Count);

                for (int j = 0; j < i; j++)
                {
                    Assert.Equal(j, series[j].Value);
                }

                s.TimeSeriesFor(doc, "speed")
                    .Append(baseline.AddMinutes(i), new []{ (double)i });

                await s.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task CanGetUpdatesTimeSeriesValuesUsingInclude_UsingQuery()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new User
                {
                    Name = "ayende"
                }, "users/1");
                await s.SaveChangesAsync();
            }

            var baseline = RavenTestHelper.UtcToday;

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();

                // collection query
                var docs = await s.Query<User>()
                    .Include(i => i.IncludeTimeSeries("speed", DateTime.MinValue, DateTime.MaxValue))
                    .ToListAsync();

                foreach (var doc in docs)
                {
                    var numOfRequests = s.Advanced.NumberOfRequests;

                    // should not go to server
                    var series = (await s.TimeSeriesFor(doc, "speed")
                        .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .ToList();
                    Assert.Equal(numOfRequests, s.Advanced.NumberOfRequests);
                    Assert.Equal(i, series.Count);

                    for (int j = 0; j < i; j++)
                    {
                        Assert.Equal(j, series[j].Value);
                    }

                    s.TimeSeriesFor(doc, "speed")
                        .Append(baseline.AddMinutes(i), new[] { (double)i });

                    await s.SaveChangesAsync();
                }
            }

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();

                // non collection query
                var docs = await s.Query<User>()
                    .Where(u => u.Name == "ayende")
                    .Include(i => i.IncludeTimeSeries("speed2", DateTime.MinValue, DateTime.MaxValue))
                    .ToListAsync();

                foreach (var doc in docs)
                {
                    var numOfRequests = s.Advanced.NumberOfRequests;

                    // should not go to server
                    var series = (await s.TimeSeriesFor(doc, "speed2")
                        .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .ToList();
                    Assert.Equal(numOfRequests, s.Advanced.NumberOfRequests);
                    Assert.Equal(i, series.Count);

                    for (int j = 0; j < i; j++)
                    {
                        Assert.Equal(j, series[j].Value);
                    }

                    s.TimeSeriesFor(doc, "speed2")
                        .Append(baseline.AddMinutes(i), new[] { (double)i });

                    await s.SaveChangesAsync();
                }
            }
        }
    }
}
