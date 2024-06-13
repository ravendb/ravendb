using System;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17134 : RavenTestBase
    {
        public RavenDB_17134(ITestOutputHelper output) : base(output)
        {
        }

        public class TimeSeriesRange
        {
            public DateTime From, To;
            public string Key;
            public long[] Count;
            public double[] Sum;
        }

        class Dog
        {
            public string Name;
            public int Age;

            public Dog(string name, int age)
            {
                Name = name;
                Age = age;
            }
        }
        class User
        {
            public Dog[] Dogs;
        } 
        
        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectNoValuesFromResult(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Dogs = new []
                    {
                        new Dog("Arava", 12), 
                        new Dog("Pheobe", 7)
                    }
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var dogs = await session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return []; }
from Users  as u
select project(u)
")
                    .Statistics(out var stats)
                    .ToListAsync();
                
                Assert.Equal(1, stats.TotalResults);
                Assert.Equal(0, dogs.Count);
            }
        } 

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectMultipleValuesFromManyResult(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 4; i++)
                {
                    await session.StoreAsync(new User
                    {
                        Dogs = new []
                        {
                            new Dog("Arava", 12), 
                            new Dog("Pheobe", 7)
                        }
                    });
                }
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var dogs = await session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return u.Dogs; }
from Users  as u
select project(u)
")
                    .Statistics(out var stats)
                    .ToListAsync();
                
                Assert.Equal(8, stats.TotalResults);
                Assert.Equal(8, dogs.Count);
            }
        } 


        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectMultipleValuesFromSingleResultInCollectionQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Dogs = new []
                    {
                        new Dog("Arava", 12), 
                        new Dog("Pheobe", 7)
                    }
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var dogs = await session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return u.Dogs; }
from Users  as u
select project(u)
")
                    .Statistics(out var stats)
                    .ToListAsync();
                
                Assert.Equal(2, stats.TotalResults);
                Assert.Equal(2, dogs.Count);
            }
        } 
        
        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectMultipleValuesFromSingleResultInIndexQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Dogs = new []
                    {
                        new Dog("Arava", 12), 
                        new Dog("Pheobe", 7)
                    }
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var dogs = await session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return u.Dogs; }
from Users  as u
where u.Dogs.Count > 0
select project(u)
")
                    .Statistics(out var stats)
                    .ToListAsync();
                
                Assert.Equal(2, stats.TotalResults);
                Assert.Equal(2, dogs.Count);
            }
        } 

        
        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectTimeSeriesInCollectionQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                User user = new User();
                await session.StoreAsync(user);

                var baseline = DateTime.Today;
                session.TimeSeriesFor(user, "walks").Append(baseline, 45);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddHours(1), 7);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddDays(1), 47);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddDays(2), 43);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var range = await session.Advanced.AsyncRawQuery<TimeSeriesRange>(@"
declare timeseries time_walked_by_day(e){
    from e.Walks group by 1d select sum()
}
declare function project(e){
    return time_walked_by_day(e).Results;
}
from Users as e
select project(e)
")
                    .Statistics(out var stats)
                    .ToListAsync();
                
                WaitForUserToContinueTheTest(store);
                Assert.Equal(3, stats.TotalResults);
                Assert.Equal(3, range.Count);
            }
        } 
        
        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectTimeSeriesInIndexQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession())
            {
                User user = new User();
                await session.StoreAsync(user);

                var baseline = DateTime.Today;
                session.TimeSeriesFor(user, "walks").Append(baseline, 45);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddHours(1), 7);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddDays(1), 47);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddDays(2), 43);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var range = await session.Advanced.AsyncRawQuery<TimeSeriesRange>(@"
declare timeseries time_walked_by_day(e){
    from e.Walks group by 1d select sum()
}
declare function project(e){
    return time_walked_by_day(e).Results;
}
from Users as e
where e.Dogs  == null
select project(e)
")
                    .Statistics(out var stats)
                    .ToListAsync();
                
                Assert.Equal(3, stats.TotalResults);
                Assert.Equal(3, range.Count);
            }
        } 
    }
}
