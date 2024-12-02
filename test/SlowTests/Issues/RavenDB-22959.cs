using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22959 : RavenTestBase
    {
        public RavenDB_22959(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        public async Task SessionLoadWithIncludeAllTimeSeriesByRange()
        {
            var baseTime = new DateTime(year: 2024, month: 1, day: 1, hour: 1, minute: 30, second: 0);

            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.Time.UtcDateTime = () => baseTime;

            var tags = new string[] { "watches/apple", "watches/galaxy", "watches/fitbit", "watches/garmin" };

            var time = baseTime;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/1", Name = "Shahar" });
                await session.SaveChangesAsync();

                for (int i = 0; i < tags.Length; i++)
                {
                    for (int t = 1; t <= 3; t++)
                    {
                        var tsf = session.TimeSeriesFor("Users/1", "TimeSeries"+t);
                        var val = (double)(t*10+i);
                        var tag = tags[i];
                        tsf.Append(time, new[] { val }, tag);
                    }
                    await session.SaveChangesAsync();

                    time += TimeSpan.FromDays(1);
                    db.Time.UtcDateTime = () => time;
                }

            }


            var from = baseTime + TimeSpan.FromDays(0.5);
            var to = baseTime + TimeSpan.FromDays(2.5);

            using (var session = store.OpenAsyncSession())
            {
                var queryResults = await session.Query<User>()
                    .Where(u => u.Id == "Users/1")
                    .Include(includeBuilder => includeBuilder.IncludeAllTimeSeries(from, to))
                    .ToListAsync();

                Assert.Equal(1, queryResults.Count);
                var user = queryResults.FirstOrDefault();

                var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync(from, to);
                Assert.Equal(2, entries1.Length);
                Assert.Equal(tags[1], entries1[0].Tag);
                Assert.Equal(tags[2], entries1[1].Tag);
                var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync(from, to);
                Assert.Equal(2, entries2.Length);
                Assert.Equal(tags[1], entries2[0].Tag);
                Assert.Equal(tags[2], entries2[1].Tag);
                var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync(from, to);
                Assert.Equal(2, entries3.Length);
                Assert.Equal(tags[1], entries3[0].Tag);
                Assert.Equal(tags[2], entries3[1].Tag);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }


            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("Users/1", includeBuilder => includeBuilder.IncludeAllTimeSeries(from, to));
                    
                var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync(from, to);
                Assert.Equal(2, entries1.Length);
                Assert.Equal(tags[1], entries1[0].Tag);
                Assert.Equal(tags[2], entries1[1].Tag);
                var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync(from, to);
                Assert.Equal(2, entries2.Length);
                Assert.Equal(tags[1], entries2[0].Tag);
                Assert.Equal(tags[2], entries2[1].Tag);
                var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync(from, to);
                Assert.Equal(2, entries3.Length);
                Assert.Equal(tags[1], entries3[0].Tag);
                Assert.Equal(tags[2], entries3[1].Tag);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }


            using (var session = store.OpenAsyncSession())
            {
                var queryResults = await session.Query<User>()
                    .Where(u => u.Id == "Users/1")
                    .Include(includeBuilder => includeBuilder.IncludeAllTimeSeries())
                    .ToListAsync();

                Assert.Equal(1, queryResults.Count);
                var user = queryResults.FirstOrDefault();

                var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync();
                Assert.Equal(4, entries1.Length);
                var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync();
                Assert.Equal(4, entries2.Length);
                var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync();
                Assert.Equal(4, entries3.Length);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }


            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("Users/1", includeBuilder => includeBuilder.IncludeAllTimeSeries());

                var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync();
                Assert.Equal(4, entries1.Length);
                var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync();
                Assert.Equal(4, entries2.Length);
                var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync();
                Assert.Equal(4, entries3.Length);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }

        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        public async Task SessionLoadWithIncludeAllTimeSeriesByRangeWithMultipleDocs()
        {
            var baseTime = new DateTime(year: 2024, month: 1, day: 1, hour: 1, minute: 30, second: 0);

            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.Time.UtcDateTime = () => baseTime;

            var ids = new string[] { "Users/1", "Users/2", "Users/3", "Users/4" };

            var tags = new string[] { "watches/apple", "watches/galaxy", "watches/fitbit", "watches/garmin" };

            var time = baseTime;



            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < ids.Length; i++)
                {
                    var name = i % 2 == 0 ? "Shahar" : "Omer";
                    await session.StoreAsync(new User { Id = ids[i], Name = name });
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < tags.Length; i++)
                {
                    for (int t = 1; t <= 3; t++)
                    {
                        foreach (var docId in ids)
                        {
                            var tsf = session.TimeSeriesFor(docId, "TimeSeries" + t);
                            var val = (double)(t * 10 + i);
                            var tag = tags[i];
                            tsf.Append(time, new[] { val }, tag);
                        }
                    }
                    await session.SaveChangesAsync();

                    time += TimeSpan.FromDays(1);
                    db.Time.UtcDateTime = () => time;
                }

            }


            var from = baseTime + TimeSpan.FromDays(0.5);
            var to = baseTime + TimeSpan.FromDays(2.5);

            using (var session = store.OpenAsyncSession())
            {
                var queryResults = await session.Query<User>()
                    .Where(u => u.Name == "Shahar")
                    .Include(includeBuilder => includeBuilder.IncludeAllTimeSeries(from, to))
                    .ToListAsync();

                Assert.Equal(2, queryResults.Count);
                foreach (var user in queryResults)
                {
                    var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync(from, to);
                    Assert.Equal(2, entries1.Length);
                    Assert.Equal(tags[1], entries1[0].Tag);
                    Assert.Equal(tags[2], entries1[1].Tag);
                    var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync(from, to);
                    Assert.Equal(2, entries2.Length);
                    Assert.Equal(tags[1], entries2[0].Tag);
                    Assert.Equal(tags[2], entries2[1].Tag);
                    var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync(from, to);
                    Assert.Equal(2, entries3.Length);
                    Assert.Equal(tags[1], entries3[0].Tag);
                    Assert.Equal(tags[2], entries3[1].Tag);
                }

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                queryResults = await session.Query<User>()
                    .Where(u => u.Name == "Omer")
                    .ToListAsync();

                foreach (var user in queryResults)
                {
                    var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync(from, to);
                    Assert.Equal(2, entries1.Length);
                    Assert.Equal(tags[1], entries1[0].Tag);
                    Assert.Equal(tags[2], entries1[1].Tag);
                    var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync(from, to);
                    Assert.Equal(2, entries2.Length);
                    Assert.Equal(tags[1], entries2[0].Tag);
                    Assert.Equal(tags[2], entries2[1].Tag);
                    var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync(from, to);
                    Assert.Equal(2, entries3.Length);
                    Assert.Equal(tags[1], entries3[0].Tag);
                    Assert.Equal(tags[2], entries3[1].Tag);
                }

                Assert.Equal(8, session.Advanced.NumberOfRequests);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var queryResults = await session.Query<User>()
                    .Where(u => u.Name == "Shahar")
                    .Include(includeBuilder => includeBuilder.IncludeAllTimeSeries())
                    .ToListAsync();

                Assert.Equal(2, queryResults.Count);
                foreach (var user in queryResults)
                {
                    var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync(from, to);
                    Assert.Equal(2, entries1.Length);
                    Assert.Equal(tags[1], entries1[0].Tag);
                    Assert.Equal(tags[2], entries1[1].Tag);
                    var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync(from, to);
                    Assert.Equal(2, entries2.Length);
                    Assert.Equal(tags[1], entries2[0].Tag);
                    Assert.Equal(tags[2], entries2[1].Tag);
                    var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync(from, to);
                    Assert.Equal(2, entries3.Length);
                    Assert.Equal(tags[1], entries3[0].Tag);
                    Assert.Equal(tags[2], entries3[1].Tag);
                }

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                queryResults = await session.Query<User>()
                    .Where(u => u.Name == "Omer")
                    .ToListAsync();

                foreach (var user in queryResults)
                {
                    var entries1 = await session.TimeSeriesFor(user, "TimeSeries1").GetAsync(from, to);
                    Assert.Equal(2, entries1.Length);
                    Assert.Equal(tags[1], entries1[0].Tag);
                    Assert.Equal(tags[2], entries1[1].Tag);
                    var entries2 = await session.TimeSeriesFor(user, "TimeSeries2").GetAsync(from, to);
                    Assert.Equal(2, entries2.Length);
                    Assert.Equal(tags[1], entries2[0].Tag);
                    Assert.Equal(tags[2], entries2[1].Tag);
                    var entries3 = await session.TimeSeriesFor(user, "TimeSeries3").GetAsync(from, to);
                    Assert.Equal(2, entries3.Length);
                    Assert.Equal(tags[1], entries3[0].Tag);
                    Assert.Equal(tags[2], entries3[1].Tag);
                }

                Assert.Equal(8, session.Advanced.NumberOfRequests);
            }

        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

        }
    }
}
