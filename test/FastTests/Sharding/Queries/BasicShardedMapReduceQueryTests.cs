using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Queries
{
    public class BasicShardedMapReduceQueryTests : RavenTestBase
    {
        public BasicShardedMapReduceQueryTests(ITestOutputHelper output) : base(output)
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

                    var queryResult = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .ToList();

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
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/3");
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var queryResult = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Filter(x => x.Sum >= 30)
                        .ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(30, queryResult[0].Sum);
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
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/1");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/2");
                    session.Store(new User { Name = "Jane", Count = 10 }, "users/3");
                    session.SaveChanges();

                    var queryResult = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
filter Count >= 30
select sum(""Count"") as Sum, key() as Name")
                        .ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(30, queryResult[0].Sum);
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

                    var queryResult = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .Statistics(out var stats)
                        .Filter(x => x.Sum >= 30)
                        .ToList();

                    Assert.Equal(0, queryResult.Count);
                    Assert.Equal(1, stats.SkippedResults);
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

                    var queryResult = session.Advanced.RawQuery<UserMapReduce.Result>(
                            @"
from Users
group by Name
filter Count >= 30
select sum(""Count"") as Sum, key() as Name")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.Equal(0, queryResult.Count);
                    Assert.Equal(1, stats.SkippedResults);
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

                    var queryResult = session.Advanced.RawQuery<AutoMapReduceResult>(
                            @"
from Orders
group by Company
order by count() desc
select count() as Count, key() as Company")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Companies/2-A", queryResult[0].Company);
                    Assert.Equal(2, queryResult[0].Count);
                    Assert.Equal("Companies/1-A", queryResult[1].Company);
                    Assert.Equal(1, queryResult[1].Count);

                    var queryResult2 = session.Advanced.RawQuery<AutoMapReduceResult2>(
                            $@"
from index '{stats.IndexName}' as o
order by o.Count
select {{
    NewCompanyName: o.Company + '_' + o.Company,
    NewCount: o.Count * 2
}}
")
                        .ToList();

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

                    var queryResult = session.Advanced.RawQuery<OrderLine>(
                            $@"
declare function project(o) {{
    return o.Lines;
}}

from index 'OrderMapReduceIndex' as o
order by Freight as {sortType}
select project(o)")
                        .ToList();

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

                    var queryResult = session.Query<UserMapReduce.Result, UserMapReduce>()
                        .OrderBy(x => x.Name)
                        .Select(x => new
                        {
                            x.Sum
                        })
                        .ToList();

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

                    var queryResult = (from user in session.Query<UserMapReduce.Result, UserMapReduce>()
                                       let sum = user.Sum + 1
                                       let name = user.Name + "_" + user.Name
                                       orderby user.Sum
                                       select new
                                       {
                                           Sum = sum,
                                           Name = name
                                       })
                        .ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(2, queryResult[0].Sum);
                    Assert.Equal("Grisha_Grisha", queryResult[0].Name);
                    Assert.Equal(3, queryResult[1].Sum);
                    Assert.Equal("Igal_Igal", queryResult[1].Name);
                    Assert.Equal(4, queryResult[2].Sum);
                    Assert.Equal("Egor_Egor", queryResult[2].Name);
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

                    var queryResult = (from user in session.Query<UserMapReduce.Result, UserMapReduce>()
                                       let sum = user.Sum
                                       orderby user.Sum
                                       select new
                                       {
                                           Sum = sum
                                       })
                        .ToList();

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

                    var exception = Assert.Throws<NotSupportedInShardingException>(() => (from user in session.Query<UserMapReduce.Result, UserMapReduce>()
                                                                         let anotherUser = RavenQuery.Load<User>(user.Name)
                                                                         select new
                                                                         {
                                                                             Name = anotherUser.Name
                                                                         })
                        .ToList());

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
                    Assert.Throws<IndexDoesNotExistException>(() => session.Query<UserMapReduce.Result, UserMapReduce>().ToList());
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
    }
}
