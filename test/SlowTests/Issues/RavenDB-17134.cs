using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value
namespace SlowTests.Issues
{
    public class RavenDB_17134 : RavenTestBase
    {
        public RavenDB_17134(ITestOutputHelper output) : base(output)
        {
        }
        
        private class TimeSeriesRange
        {
            public DateTime From, To;
            public string Key;
            public long[] Count;
            public double[] Sum;

            public override bool Equals(object obj)
            {
                if (obj is TimeSeriesRange tsr)
                {
                    Assert.Equal(From, tsr.From);
                    Assert.Equal(To, tsr.To);
                    Assert.Equal(Key, tsr.Key);
                    Assert.Equal(Count, tsr.Count);
                    Assert.Equal(Sum, tsr.Sum);
                    return true;
                }
                
                return base.Equals(obj);
            }
        }

        private class Dog
        {
            public string Name;
            public int Age;

            public Dog(string name, int age)
            {
                Name = name;
                Age = age;
            }

            public override bool Equals(object obj)
            {
                if (obj is Dog dog)
                {
                    Assert.Equal(Name, dog.Name);
                    Assert.Equal(Age, dog.Age);
                    return true;
                }
                
                return base.Equals(obj);
            }
        }
        private class User
        {
            public Dog[] Dogs;
            public string Id;

            public override bool Equals(object obj)
            {
                if (obj is User user)
                {
                    Assert.Equal(Dogs.Length, user.Dogs.Length);

                    for (int i = 0; i < Dogs.Length; ++i)
                        Dogs[i].Equals(user.Dogs[i]);
                    return true;
                }
                
                return base.Equals(obj);
            }
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
                var query = session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return []; }
from Users  as u
select project(u)
");
                var (dogs, stats) = await GetResultAndAssertWithStreaming(session, query);
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
                var query = session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return u.Dogs; }
from Users  as u
select project(u)
");
                var (dogs, stats) = await GetResultAndAssertWithStreaming(session, query);
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
                var query = session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return u.Dogs; }
from Users  as u
select project(u)
");
                var (dogs, stats) = await GetResultAndAssertWithStreaming(session, query);
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
                var query = session.Advanced.AsyncRawQuery<Dog>(@"
declare function project(u) { return u.Dogs; }
from Users  as u
where u.Dogs.Count > 0
select project(u)
");
                var (dogs, stats) = await GetResultAndAssertWithStreaming(session, query);
                
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
                var query = session.Advanced.AsyncRawQuery<TimeSeriesRange>(@"
declare timeseries time_walked_by_day(e){
    from e.Walks group by 1d select sum()
}
declare function project(e){
    return time_walked_by_day(e).Results;
}
from Users as e
select project(e)
");
                var (range, stats) = await GetResultAndAssertWithStreaming(session, query);
                
                
                Assert.Equal(3, stats.TotalResults);
                Assert.Equal(3, range.Count);
            }
        } 
        
        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanProjectTimeSeriesInCollectionQueryWithLoad(Options options)
        {
            using var store = GetDocumentStore(options);
            User user = new User();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);

                var baseline = DateTime.Today;
                session.TimeSeriesFor(user, "walks").Append(baseline, 45);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddHours(1), 7);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddDays(1), 47);
                session.TimeSeriesFor(user, "walks").Append(baseline.AddDays(2), 43);
                await session.SaveChangesAsync();

                var dog1 = new Dog(name: "user.Id", -1);
                var dog2 = new Dog(name: "", -1);
                await session.StoreAsync(dog1);
                await session.StoreAsync(dog2);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var query = session.Advanced.AsyncRawQuery<TimeSeriesRange>(@$"
declare timeseries time_walked_by_day(e){{
    from e.Walks group by 1d select sum()
}}
declare function project(e){{
    return time_walked_by_day(e).Results;
}}
from Dogs as d
load ""{user.Id}"" as e
select project(e)
");
                var (range, stats) = await GetResultAndAssertWithStreaming(session, query);
                
                
                Assert.Equal(6, stats.TotalResults);
                Assert.Equal(6, range.Count);
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
                var query = session.Advanced.AsyncRawQuery<TimeSeriesRange>(@"
declare timeseries time_walked_by_day(e){
    from e.Walks group by 1d select sum()
}
declare function project(e){
    return time_walked_by_day(e).Results;
}
from Users as e
where e.Dogs  == null
select project(e)
");
                var (range, stats) = await GetResultAndAssertWithStreaming(session, query); 
                Assert.Equal(3, stats.TotalResults);
                Assert.Equal(3, range.Count);
            }
        }

        private static async Task<(List<T>, QueryStatistics)> GetResultAndAssertWithStreaming<T>(IAsyncDocumentSession session, IAsyncRawDocumentQuery<T> rawDocumentQuery)
        {
            List<T> result = await rawDocumentQuery.Statistics(out var statistics).ToListAsync();
            List<T> streamResult = new();
            var stream = await session.Advanced.StreamAsync<T>(rawDocumentQuery);
            while (await stream.MoveNextAsync())
            {
                streamResult.Add(stream.Current.Document);
            }
            
            Assert.Equal(result.Count, streamResult.Count);
            for (int i = 0; i < result.Count; ++i)
                Assert.True(result[i].Equals(streamResult[i]));
            
            
            return (result, statistics) ;
        }
    }
}
#pragma warning restore CS0649 // Field is never assigned to, and will always have its default value
