using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;

namespace FastTests.Sharding.Queries
{
    public class BasicShardedMapReduceStreamingTests : RavenTestBase
    {
        public BasicShardedMapReduceStreamingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Simple_Map_Reduce()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 1 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 2 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 3 }, "users/3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<UserMapReduce.Result, UserMapReduce>();

                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query))
                    {
                        while (stream.MoveNext())
                        {
                            queryResult.Add(stream.Current.Document);
                        }
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(6, queryResult[0].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_With_Filter()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/5");
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/6");
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/7");
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/8");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/3");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/4");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query1 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 40);

                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(40, queryResult[0].Sum);

                    var query2 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 40)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(40, queryResult[0].Sum);

                    var query3 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 40)
                        .OrderBy(x => x.Name)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query3))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(40, queryResult[0].Sum);

                    var query4 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 20)
                        .OrderBy(x => x.Name);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query4))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal(36, queryResult[0].Sum);
                    Assert.Equal("Jane", queryResult[1].Name);
                    Assert.Equal(40, queryResult[1].Sum);

                    var query5 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 20)
                        .OrderBy(x => x.Name)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query5))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal(36, queryResult[0].Sum);

                    var query6 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 20)
                        .OrderByDescending(x => x.Name)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query6))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(40, queryResult[0].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Auto_Map_Reduce_With_Filter()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/5");
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/6");
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/7");
                    session.Store(new User { Name = "Grisha", Count = 9 }, "users/8");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/3");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/4");
                    session.SaveChanges();

                    var query1 = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
filter Count >= 40
select sum(""Count"") as Sum, key() as Name");

                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(40, queryResult[0].Sum);

                    var query2 = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
filter Count >= 40
select sum(""Count"") as Sum, key() as Name
limit 1");

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(40, queryResult[0].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_With_Filter_No_Result()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 9 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 9 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 9 }, "users/3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 30);

                    var queryResult = new List<UserMapReduce.Result>();

                    // Streaming with Statistics
                    using (var stream = session.Advanced.Stream(query, out var stats))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);

                        Assert.Equal(0, queryResult.Count);
                        // Accessing stats is only safe after iteration or inside the block, 
                        // but SkippedResults is populated as we go or at the end.
                        //Assert.Equal(1, stats.SkippedResults);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Auto_Map_Reduce_With_Filter_No_Result()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 9 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 9 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 9 }, "users/3");
                    session.SaveChanges();

                    var query = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
filter Count >= 30
select sum(""Count"") as Sum, key() as Name");

                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query, out var stats))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);

                        Assert.Equal(0, queryResult.Count);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_With_Limit()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());
                store.ExecuteIndex(new UserMapReduceWithTwoReduceKeys());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", LastName = "Kotler", Count = 21 }, "users/3$3");
                    session.Store(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/4$3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query1 = session.Query<UserMapReduce.Result, UserMapReduce>().Take(1);
                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query2 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Skip(1)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(30, queryResult[0].Sum);

                    var query3 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                            .Take(1);

                    var queryResult2 = new List<UserMapReduceWithTwoReduceKeys.Result>();
                    using (var stream = session.Advanced.Stream(query3))
                    {
                        while (stream.MoveNext())
                            queryResult2.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult2.Count);
                    Assert.Equal("Grisha", queryResult2[0].Name);
                    Assert.Equal("Kotler", queryResult2[0].LastName);
                    Assert.Equal(21, queryResult2[0].Sum);

                    var query4 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .Skip(1)
                        .Take(1);

                    queryResult2.Clear();
                    using (var stream = session.Advanced.Stream(query4))
                    {
                        while (stream.MoveNext())
                            queryResult2.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult2.Count);
                    Assert.Equal("Jane", queryResult2[0].Name);
                    Assert.Equal("Doe", queryResult2[0].LastName);
                    Assert.Equal(30, queryResult2[0].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Auto_Map_Reduce_With_Limit()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", LastName = "Kotler", Count = 21 }, "users/3$3");
                    session.Store(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/4$3");
                    session.SaveChanges();

                    var query1 = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
select sum(Count) as Sum, key() as Name
limit 1");

                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query2 = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
select sum(Count) as Sum, key() as Name
limit 1, 1");

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(30, queryResult[0].Sum);

                    var query3 = session.Advanced.RawQuery<UserMapReduceWithTwoReduceKeys.Result>(
                            @"
from Users
group by Name
select sum(Count) as Sum, Name, LastName
limit 1");

                    var queryResult2 = new List<UserMapReduceWithTwoReduceKeys.Result>();
                    using (var stream = session.Advanced.Stream(query3))
                    {
                        while (stream.MoveNext())
                            queryResult2.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult2.Count);
                    Assert.Equal("Grisha", queryResult2[0].Name);
                    Assert.Equal(21, queryResult2[0].Sum);

                    var query4 = session.Advanced.RawQuery<UserMapReduceWithTwoReduceKeys.Result>(
                            @"
from Users
group by Name
select sum(Count) as Sum, Name, LastName
limit 1, 1");

                    queryResult2.Clear();
                    using (var stream = session.Advanced.Stream(query4))
                    {
                        while (stream.MoveNext())
                            queryResult2.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult2.Count);
                    Assert.Equal("Jane", queryResult2[0].Name);
                    Assert.Equal(30, queryResult2[0].Sum);

                    var query5 = session.Advanced.RawQuery<UserMapReduceWithTwoReduceKeys.CompoundResult>(
                            @"
from Users
group by Name, LastName
select sum(Count) as Sum, key() as Name
limit 1");

                    var queryResult3 = new List<UserMapReduceWithTwoReduceKeys.CompoundResult>();
                    using (var stream = session.Advanced.Stream(query5))
                    {
                        while (stream.MoveNext())
                            queryResult3.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult3.Count);
                    var properties = (IDictionary<string, object>)queryResult3[0].Name;
                    Assert.Equal("Grisha", properties[nameof(UserMapReduceWithTwoReduceKeys.Result.Name)]);
                    Assert.Equal("Kotler", properties[nameof(UserMapReduceWithTwoReduceKeys.Result.LastName)]);
                    Assert.Equal(21, queryResult3[0].Sum);

                    var query6 = session.Advanced.RawQuery<UserMapReduceWithTwoReduceKeys.CompoundResult>(
                            @"
from Users
group by Name, LastName
select sum(Count) as Sum, key() as Name
limit 1, 1");

                    queryResult3.Clear();
                    using (var stream = session.Advanced.Stream(query6))
                    {
                        while (stream.MoveNext())
                            queryResult3.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult3.Count);
                    properties = (IDictionary<string, object>)queryResult3[0].Name;
                    Assert.Equal("Jane", properties[nameof(UserMapReduceWithTwoReduceKeys.Result.Name)]);
                    Assert.Equal("Doe", properties[nameof(UserMapReduceWithTwoReduceKeys.Result.LastName)]);
                    Assert.Equal(30, queryResult3[0].Sum);

                    var query7 = session.Query<User>()
                        .GroupBy(x => new { x.Name, x.LastName }).Select(x => new
                        {
                            Name = x.Key.Name,
                            LastName = x.Key.LastName,
                            Sum = x.Sum(u => u.Count)
                        })
                        .Take(1);

                    // Note: Anonymous types in streaming come back as JObject/Expando, 
                    // but the wrapper handles projection mapping if done via Query<T>. 
                    // However, we need to capture statistics first to get the IndexName.

                    StreamQueryStatistics stats;
                    // We must execute this to get the stats (auto index name).
                    // Streaming this specific anonymous projection might need a concrete type for easy List add,
                    // but let's assume dynamic for the test or create a helper class.
                    // The original test uses `ToList` returning anonymous objects.
                    // Streaming supports this.

                    var autoLinqResultList = new List<dynamic>();
                    using (var stream = session.Advanced.Stream(query7, out stats))
                    {
                        while (stream.MoveNext())
                            autoLinqResultList.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, autoLinqResultList.Count);
                    // Use dynamic access or casting
                    Assert.Equal("Grisha", autoLinqResultList[0].Name);
                    Assert.Equal("Kotler", autoLinqResultList[0].LastName);
                    Assert.Equal(21, autoLinqResultList[0].Sum);

                    // Re-querying using the index name from stats
                    var query8 = session.Query<User>(stats.IndexName)
                        .Take(1)
                        .As<AutoMapReduceResult3>();

                    var autoIndexResult = new List<AutoMapReduceResult3>();
                    using (var stream = session.Advanced.Stream(query8))
                    {
                        while (stream.MoveNext())
                            autoIndexResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, autoIndexResult.Count);
                    Assert.Equal("Grisha", autoIndexResult[0].Name);
                    Assert.Equal("Kotler", autoIndexResult[0].LastName);
                    Assert.Equal(21, autoIndexResult[0].Count);

                    var query9 = session.Query<User>(stats.IndexName)
                        .OrderBy(x => x.Name)
                        .Take(1)
                        .As<AutoMapReduceResult3>();

                    autoIndexResult.Clear();
                    using (var stream = session.Advanced.Stream(query9))
                    {
                        while (stream.MoveNext())
                            autoIndexResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, autoIndexResult.Count);
                    Assert.Equal("Grisha", autoIndexResult[0].Name);
                    Assert.Equal("Kotler", autoIndexResult[0].LastName);
                    Assert.Equal(21, autoIndexResult[0].Count);

                    var query10 = session.Query<User>(stats.IndexName)
                        .OrderByDescending(x => x.Name)
                        .Take(1)
                        .As<AutoMapReduceResult3>();

                    autoIndexResult.Clear();
                    using (var stream = session.Advanced.Stream(query10))
                    {
                        while (stream.MoveNext())
                            autoIndexResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, autoIndexResult.Count);
                    Assert.Equal("Jane", autoIndexResult[0].Name);
                    Assert.Equal("Doe", autoIndexResult[0].LastName);
                    Assert.Equal(30, autoIndexResult[0].Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_With_Order_By_On_Non_Reduce_Key()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());
                store.ExecuteIndex(new UserMapReduceJs());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", Count = 21 }, "users/3$3");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/4$3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query1 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .OrderByDescending(x => x.Sum)
                        .Take(1);

                    var error = Assert.Throws<NotSupportedInShardingException>(() => session.Advanced.Stream(query1));
                    Assert.Contains("Ordering by field 'Sum' which is not part of the 'group by' clause is not supported in sharded streaming queries.", error.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Auto_Map_Reduce_With_Order_By_On_Non_Reduce_Key()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", Count = 21 }, "users/3$3");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/4$3");
                    session.SaveChanges();

                    var query1 = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
order by Count as long desc
select sum(""Count"") as Sum, key() as Name");

                    var error = Assert.Throws<NotSupportedInShardingException>(() => session.Advanced.Stream(query1));
                    Assert.Contains("Ordering by field 'Count' which is not part of the 'group by' clause is not supported in sharded streaming queries.", error.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_With_Order_By_On_Reduce_Key_With_Take()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());
                store.ExecuteIndex(new UserMapReduceJs());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", Count = 21 }, "users/3$3");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/4$3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query1 = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .OrderByDescending(x => x.Name)
                        .Take(1);

                    var queryResult = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(30, queryResult[0].Sum);

                    var query2 = session.Query<UserMapReduceJs.Result, UserMapReduceJs>()
                        .OrderByDescending(x => x.Name)
                        .Take(1);

                    var queryResultJs = new List<UserMapReduceJs.Result>();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResultJs.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResultJs.Count);
                    Assert.Equal("Jane", queryResultJs[0].Name);
                    Assert.Equal(30, queryResultJs[0].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Auto_Map_Reduce_With_Order_By_On_Reduce_Key_With_Take()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", Count = 21 }, "users/3$3");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/4$3");
                    session.SaveChanges();

                    var query1 = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
order by Name desc
select sum(""Count"") as Sum, key() as Name
limit 1");

                    var queryResult = new List<UserMapReduce.Result>();
                    StreamQueryStatistics stats;
                    using (var stream = session.Advanced.Stream(query1, out stats))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jane", queryResult[0].Name);
                    Assert.Equal(30, queryResult[0].Sum);

                    WaitForUserToContinueTheTest(store);

                    var query2 = session.Advanced.RawQuery<UserMapReduce.Result>(
                    $@"
from index ""{stats.IndexName}"" as o
order by o.Name desc
select {{
    Name: o.Name,
    Sum: o.Count
}}
limit 1
");
                    var queryResult2 = new List<UserMapReduce.Result>();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult2.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult2.Count);
                    Assert.Equal("Jane", queryResult2[0].Name);
                    Assert.Equal(30, queryResult2[0].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_With_Order_By_On_Reduce_Keys_With_Take()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduceWithTwoReduceKeys());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", LastName = "Kotler", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Grisha", LastName = "Kotler", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Grisha", LastName = "Kotler", Count = 10 }, "users/4$3");
                    session.Store(new User { Name = "Grisha", LastName = "A", Count = 21 }, "users/3$3");

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query1 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .Take(1);

                    var queryResult = new List<UserMapReduceWithTwoReduceKeys.Result>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("A", queryResult[0].LastName);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query2 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .OrderByDescending(x => x.Name)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("A", queryResult[0].LastName);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query3 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .OrderBy(x => x.Name)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query3))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("A", queryResult[0].LastName);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query4 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .OrderBy(x => x.Name)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query4))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("A", queryResult[0].LastName);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query5 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .OrderByDescending(x => x.Name)
                        .ThenBy(x => x.LastName)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query5))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("A", queryResult[0].LastName);
                    Assert.Equal(21, queryResult[0].Sum);

                    var query6 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .OrderBy(x => x.Name)
                        .ThenByDescending(x => x.LastName)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query6))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("Kotler", queryResult[0].LastName);
                    Assert.Equal(30, queryResult[0].Sum);

                    var query7 = session.Query<UserMapReduceWithTwoReduceKeys.Result, UserMapReduceWithTwoReduceKeys>()
                        .OrderByDescending(x => x.LastName)
                        .Take(1);

                    queryResult.Clear();
                    using (var stream = session.Advanced.Stream(query7))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Grisha", queryResult[0].Name);
                    Assert.Equal("Kotler", queryResult[0].LastName);
                    Assert.Equal(30, queryResult[0].Sum);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, SearchEngineMode = RavenSearchEngineMode.All)]
        public void Auto_Map_Reduce_With_Order_By(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "Companies/1-A",
                    });
                    session.Store(new Order
                    {
                        Company = "Companies/2-A",
                    });
                    session.Store(new Order
                    {
                        Company = "Companies/2-A",
                    });
                    session.SaveChanges();

                    var query1 = session.Advanced.RawQuery<AutoMapReduceResult>(
                            @"
from Orders
group by Company
order by Company desc
select count() as Count, key() as Company");

                    var queryResult = new List<AutoMapReduceResult>();
                    StreamQueryStatistics stats;
                    using (var stream = session.Advanced.Stream(query1, out stats))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Companies/2-A", queryResult[0].Company);
                    Assert.Equal(2, queryResult[0].Count);
                    Assert.Equal("Companies/1-A", queryResult[1].Company);
                    Assert.Equal(1, queryResult[1].Count);

                    var query2 = session.Advanced.RawQuery<AutoMapReduceResult2>(
                            $@"
from index '{stats.IndexName}' as o
select {{
    NewCompanyName: o.Company + '_' + o.Company,
    NewCount: o.Count * 2
}}
");
                    var queryResult2 = new List<AutoMapReduceResult2>();
                    using (var stream = session.Advanced.Stream(query2))
                    {
                        while (stream.MoveNext())
                            queryResult2.Add(stream.Current.Document);
                    }

                    Assert.Equal(2, queryResult2.Count);
                    Assert.Equal("Companies/1-A_Companies/1-A", queryResult2[0].NewCompanyName);
                    Assert.Equal(2, queryResult2[0].NewCount);
                    Assert.Equal("Companies/2-A_Companies/2-A", queryResult2[1].NewCompanyName);
                    Assert.Equal(4, queryResult2[1].NewCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData("long", DatabaseMode = RavenDatabaseMode.Sharded, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("double", DatabaseMode = RavenDatabaseMode.Sharded, SearchEngineMode = RavenSearchEngineMode.All)]
        public void Map_Reduce_Index_With_Order_By_Multiple_Results(Options options, string sortType)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new OrderMapReduceIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Freight = 20m,
                        Lines = new List<OrderLine>
                        {
                            new()
                            {
                                Discount = 0.2m
                            },
                            new()
                            {
                                Discount = 0.4m
                            }
                        }
                    });
                    session.Store(new Order
                    {
                        Freight = 10m,
                        Lines = new List<OrderLine>
                        {
                            new()
                            {
                                Discount = 0.3m
                            },
                            new()
                            {
                                Discount = 0.5m
                            }
                        }
                    });
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query1 = session.Advanced.RawQuery<OrderLine>(
                            $@"
declare function project(o) {{
    return o.Lines;
}}

from index 'OrderMapReduceIndex' as o
order by Freight as {sortType}
select project(o)");

                    var queryResult = new List<OrderLine>();
                    using (var stream = session.Advanced.Stream(query1))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(4, queryResult.Count);
                    Assert.Equal(0.3m, queryResult[0].Discount);
                    Assert.Equal(0.5m, queryResult[1].Discount);
                    Assert.Equal(0.2m, queryResult[2].Discount);
                    Assert.Equal(0.4m, queryResult[3].Discount);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Simple_Map_Reduce_With_Order_By_And_Projection()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Count = 1 }, "users/1");
                    session.Store(new User { Name = "Igal", Count = 2 }, "users/2");
                    session.Store(new User { Name = "Egor", Count = 3 }, "users/3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .OrderBy(x => x.Name)
                        .Select(x => new
                        {
                            x.Sum
                        });

                    var queryResult = new List<dynamic>();
                    using (var stream = session.Advanced.Stream(query))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(3, queryResult[0].Sum);
                    Assert.Equal(1, queryResult[1].Sum);
                    Assert.Equal(2, queryResult[2].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Simple_Map_Reduce_With_Order_By_And_Projection2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Count = 1 }, "users/1");
                    session.Store(new User { Name = "Igal", Count = 2 }, "users/2");
                    session.Store(new User { Name = "Egor", Count = 3 }, "users/3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query = (from user in session.Query<UserMapReduce.Result, UserMapReduce>()
                        let sum = user.Sum + 1
                        let name = user.Name + "_" + user.Name
                        orderby user.Name
                        select new
                        {
                            Sum = sum,
                            Name = name
                        });

                    var queryResult = new List<dynamic>();
                    using (var stream = session.Advanced.Stream(query))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(2, queryResult[1].Sum);
                    Assert.Equal("Egor_Egor", queryResult[0].Name);
                    Assert.Equal(4, queryResult[0].Sum);
                    Assert.Equal("Grisha_Grisha", queryResult[1].Name);
                    Assert.Equal(3, queryResult[2].Sum);
                    Assert.Equal("Igal_Igal", queryResult[2].Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Simple_Map_Reduce_With_Order_By_Projecting_New_Fields()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Count = 1 }, "users/1");
                    session.Store(new User { Name = "Igal", Count = 2 }, "users/2");
                    session.Store(new User { Name = "Egor", Count = 3 }, "users/3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query = (from user in session.Query<UserMapReduce.Result, UserMapReduce>()
                                 let sum = user.Sum
                                 select new
                                 {
                                     Sum = sum
                                 });

                    var queryResult = new List<dynamic>();
                    using (var stream = session.Advanced.Stream(query))
                    {
                        while (stream.MoveNext())
                            queryResult.Add(stream.Current.Document);
                    }

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(1, queryResult[0].Sum);
                    Assert.Equal(2, queryResult[1].Sum);
                    Assert.Equal(3, queryResult[2].Sum);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Map_Reduce_Projection_With_Load_Not_Supported()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Count = 1 }, "users/1");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var query = (from user in session.Query<UserMapReduce.Result, UserMapReduce>()
                                 let anotherUser = RavenQuery.Load<User>(user.Name)
                                 select new
                                 {
                                     Name = anotherUser.Name
                                 });

                    // Streaming also throws NotSupported for Load in projections with sharding/map-reduce scenarios usually
                    var exception = Assert.Throws<NotSupportedInShardingException>(() =>
                    {
                        using (var stream = session.Advanced.Stream(query))
                        {
                            while (stream.MoveNext())
                            { }
                        }
                    });

                    Assert.Contains(nameof(NotSupportedInShardingException), exception.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public void Query_An_Index_That_Doesnt_Exist()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<UserMapReduce.Result, UserMapReduce>();
                    Assert.Throws<IndexDoesNotExistException>(() =>
                    {
                        using (var stream = session.Advanced.Stream(query))
                        {
                            while (stream.MoveNext())
                            { }
                        }
                    });
                }
            }
        }

        private class OrderMapReduceIndex : AbstractIndexCreationTask<Order>
        {
            public class Result
            {
                public decimal Freight;
                public List<OrderLine> Lines;
            }

            public OrderMapReduceIndex()
            {
                Map = orders =>
                    from order in orders
                    select new Result
                    {
                        Freight = order.Freight,
                        Lines = order.Lines
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Freight into g
                    select new Result
                    {
                        Freight = g.Key,
                        Lines = g.SelectMany(x => x.Lines).ToList()
                    };

                Index(x => x.Lines, FieldIndexing.No);
            }
        }

        private class UserMapReduce : AbstractIndexCreationTask<User, UserMapReduce.Result>
        {
            public class Result
            {
                public string Name;
                public int Sum;
            }

            public UserMapReduce()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        Name = user.Name,
                        Sum = user.Count
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Name
                    into g
                    select new Result
                    {
                        Name = g.Key,
                        Sum = g.Sum(x => x.Sum)
                    };
            }
        }

        private class UserMapReduceJs : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
#pragma warning disable CS0649
                public string Name;
                public int Sum;
#pragma warning restore CS0649
            }

            public UserMapReduceJs()
            {
                Maps = new HashSet<string>
                {
                    @"map('Users', function (c) {

                        return {
                            Name: c.Name,
                            Sum: c.Count
                        };
                    })",
                };

                Reduce = @"groupBy(x => ({
                        Name: x.Name
                    })).aggregate(g => {
                    return {
                        Name: g.key.Name,
                        Sum: g.values.reduce((res, val) => res + val.Sum, 0)
                    };
                })";
            }
        }

        private class UserMapReduceWithTwoReduceKeys : AbstractIndexCreationTask<User, UserMapReduceWithTwoReduceKeys.Result>
        {
            public class Result
            {
                public string Name;
                public string LastName;
                public int Sum;
            }

            public class CompoundResult
            {
#pragma warning disable CS0649
                public ExpandoObject Name;
                public int Sum;
#pragma warning restore CS0649
            }

            public UserMapReduceWithTwoReduceKeys()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        Name = user.Name,
                        LastName = user.LastName,
                        Sum = user.Count
                    };

                Reduce = results =>
                    from result in results
                    group result by new { result.Name, result.LastName }
                    into g
                    select new Result
                    {
                        Name = g.Key.Name,
                        LastName = g.Key.LastName,
                        Sum = g.Sum(x => x.Sum)
                    };
            }
        }

        private class AutoMapReduceResult
        {
            public string Company { get; set; }

            public int Count { get; set; }
        }

        private class AutoMapReduceResult2
        {
            public string NewCompanyName { get; set; }

            public int NewCount { get; set; }
        }

        private class AutoMapReduceResult3
        {
            public string Name { get; set; }

            public string LastName { get; set; }

            public int Count { get; set; }
        }
    }
}
