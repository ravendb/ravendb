using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class BasicShardedQueryTests : ShardedTestBase
    {
        public BasicShardedQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RawQuery_with_transformation_function_should_work()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Acme Inc."
                    }, "Companies/1");

                    session.Store(new Company
                    {
                        Name = "Evil Corp"
                    }, "Companies/2");

                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{ PricePerUnit = (decimal)1.0, Quantity = 3 },
                            new OrderLine{ PricePerUnit = (decimal)1.5, Quantity = 3 }
                        }
                    });
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{ PricePerUnit = (decimal)1.0, Quantity = 5 },
                        }
                    });
                    session.Store(new Order
                    {
                        Company = "companies/2",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{ PricePerUnit = (decimal)3.0, Quantity = 6, Discount = (decimal)3.5},
                            new OrderLine{ PricePerUnit = (decimal)8.0, Quantity = 3, Discount = (decimal)3.5},
                            new OrderLine{ PricePerUnit = (decimal)1.8, Quantity = 2 }
                        }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<dynamic>(@"
                        DECLARE function companyNameAndTotalSumSpent(o)
                        {
                          var totalSumInLines = 0;
                          for(var i = 0; i < o.Lines.length; i++)
                          {
                              var l = o.Lines[i];
                              totalSumInLines = l.PricePerUnit * l.Quantity - l.Discount;
                          }
                        
                          var company = load(o.Company);   
  
                          return { OrderedAt: o.OrderedAt, CompanyName: company.Name, TotalSumSpent: totalSumInLines };
                        }
                        
                        FROM Orders as o 
                        SELECT companyNameAndTotalSumSpent(o)                           
                    ").ToList();

                    Assert.NotEmpty(rawQuery);
                    Assert.Equal(3, rawQuery.Count);
                    Assert.DoesNotContain(rawQuery, item => item == null);

                    foreach (var item in rawQuery)
                    {
                        Assert.True((string)item.CompanyName == "Acme Inc." || (string)item.CompanyName == "Evil Corp");
                    }
                }
            }
        }

        [Fact]
        public void LinqQuery_with_transformation_function_should_work()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Acme Inc."
                    }, "Companies/1");

                    session.Store(new Company
                    {
                        Name = "Evil Corp"
                    }, "Companies/2");

                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{ PricePerUnit = (decimal)1.0, Quantity = 3 },
                            new OrderLine{ PricePerUnit = (decimal)1.5, Quantity = 3 }
                        }
                    });
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{ PricePerUnit = (decimal)1.0, Quantity = 5 },
                        }
                    });
                    session.Store(new Order
                    {
                        Company = "companies/2",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{ PricePerUnit = (decimal)3.0, Quantity = 6, Discount = (decimal)3.5},
                            new OrderLine{ PricePerUnit = (decimal)8.0, Quantity = 3, Discount = (decimal)3.5},
                            new OrderLine{ PricePerUnit = (decimal)1.8, Quantity = 2 }
                        }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var complexLinqQuery =
                        (from o in session.Query<Order>()
                         let TotalSpentOnOrder =
                             (Func<Order, decimal>)(order =>
                                 order.Lines.Sum(l => l.PricePerUnit * l.Quantity - l.Discount))
                         select new
                         {
                             OrderId = o.Id,
                             TotalMoneySpent = TotalSpentOnOrder(o),
                             CompanyName = session.Load<Company>(o.Company).Name
                         }).ToList();

                    Assert.NotEmpty(complexLinqQuery);
                    Assert.Equal(3, complexLinqQuery.Count);
                    Assert.DoesNotContain(complexLinqQuery, item => item == null);

                    foreach (var item in complexLinqQuery)
                    {
                        Assert.True((string)item.CompanyName == "Acme Inc." || (string)item.CompanyName == "Evil Corp");
                    }
                }
            }
        }

 


        [Fact]
        public void Query_With_Customize()
        {
            using (var store = GetShardedDocumentStore())
            {
                new DogsIndex().Execute(store);

                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Dog { Name = "Snoopy", Breed = "Beagle", Color = "White", Age = 6, IsVaccinated = true }, "dogs/1");
                    newSession.Store(new Dog { Name = "Brian", Breed = "Labrador", Color = "White", Age = 12, IsVaccinated = false }, "dogs/2");
                    newSession.Store(new Dog { Name = "Django", Breed = "Jack Russel", Color = "Black", Age = 3, IsVaccinated = true }, "dogs/3");
                    newSession.Store(new Dog { Name = "Beethoven", Breed = "St. Bernard", Color = "Brown", Age = 1, IsVaccinated = false }, "dogs/4");
                    newSession.Store(new Dog { Name = "Scooby Doo", Breed = "Great Dane", Color = "Brown", Age = 0, IsVaccinated = false }, "dogs/5");
                    newSession.Store(new Dog { Name = "Old Yeller", Breed = "Black Mouth Cur", Color = "White", Age = 2, IsVaccinated = true }, "dogs/6");
                    newSession.Store(new Dog { Name = "Benji", Breed = "Mixed", Color = "White", Age = 0, IsVaccinated = false }, "dogs/7");
                    newSession.Store(new Dog { Name = "Lassie", Breed = "Collie", Color = "Brown", Age = 6, IsVaccinated = true }, "dogs/8");

                    newSession.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    var queryResult = newSession.Query<DogsIndex.Result, DogsIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
                        .Where(x => x.Age > 2)
                        .ToList();

                    Assert.Equal(queryResult[0].Name, "Brian");
                    Assert.Equal(queryResult[1].Name, "Django");
                    Assert.Equal(queryResult[2].Name, "Lassie");
                    Assert.Equal(queryResult[3].Name, "Snoopy");
                }
            }
        }

        [Fact]
        public void Simple_Projection_With_Order_By()
        {
            using (var store = GetShardedDocumentStore())
            {
                store.ExecuteIndex(new UserMapIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Age = 1 }, "users/1");
                    session.Store(new User { Name = "Igal", Age = 2 }, "users/2");
                    session.Store(new User { Name = "Egor", Age = 3 }, "users/3");
                    session.SaveChanges();

                    Thread.Sleep(5000);

                    var queryResult = session.Query<UserMapIndex.Result, UserMapIndex>()
                        .OrderBy(x => x.Name)
                        .As<User>()
                        .Select(x => new
                        {
                            x.Age
                        })
                        .ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(3, queryResult[0].Age);
                    Assert.Equal(1, queryResult[1].Age);
                    Assert.Equal(2, queryResult[2].Age);
                }
            }
        }

        [Fact]
        public void Simple_Projection_With_Order_By2()
        {
            using (var store = GetShardedDocumentStore())
            {
                store.ExecuteIndex(new UserMapIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Age = 1 }, "users/1");
                    session.Store(new User { Name = "Igal", Age = 2 }, "users/2");
                    session.Store(new User { Name = "Egor", Age = 3 }, "users/3");
                    session.SaveChanges();

                    Thread.Sleep(5000);

                    var queryResult = (from user in session.Query<User, UserMapIndex>()
                        let age = user.Age
                        orderby user.Name
                        select new AgeResult
                        {
                            Age = age
                        })
                        .ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(3, queryResult[0].Age);
                    Assert.Equal(1, queryResult[1].Age);
                    Assert.Equal(2, queryResult[2].Age);
                }
            }
        }

        [Fact]
        public void Simple_Projection_With_Order_By_And_Raw_Query()
        {
            using (var store = GetShardedDocumentStore())
            {
                store.ExecuteIndex(new UserMapIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Grisha", Age = 1 }, "users/1");
                    session.Store(new User { Name = "Igal", Age = 2 }, "users/2");
                    session.Store(new User { Name = "Egor", Age = 3 }, "users/3");
                    session.SaveChanges();

                    Thread.Sleep(5000);

                    var queryResult = (session.Advanced.RawQuery<AgeResult>(@$"from index {new UserMapIndex().IndexName} as user
order by user.Name
select {{
    Age: user.Age
}}
")).ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(3, queryResult[0].Age);
                    Assert.Equal(1, queryResult[1].Age);
                    Assert.Equal(2, queryResult[2].Age);
                }
            }
        }

        [Fact]
        public void Simple_Map_Reduce()
        {
            using (var store = GetShardedDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "Jane", Count = 1 }, "users/1");
                    newSession.Store(new User { Name = "Jane", Count = 2 }, "users/2");
                    newSession.Store(new User { Name = "Jane", Count = 3 }, "users/3");
                    newSession.SaveChanges();

                    Thread.Sleep(5000);
                    var queryResult = newSession.Query<UserMapReduce.Result, UserMapReduce>()
                        .ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal(6, queryResult[0].Sum);
                }
            }
        }

        [Fact]
        public void Simple_Map_Reduce_With_Order_By_And_Projection()
        {
            using (var store = GetShardedDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "Grisha", Count = 1 }, "users/1");
                    newSession.Store(new User { Name = "Igal", Count = 2 }, "users/2");
                    newSession.Store(new User { Name = "Egor", Count = 3 }, "users/3");
                    newSession.SaveChanges();

                    Thread.Sleep(5000);

                    var queryResult = newSession.Query<UserMapReduce.Result, UserMapReduce>()
                        .OrderBy(x => x.Sum)
                        .Select(x => new
                        {
                            x.Sum
                        })
                        .ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(1, queryResult[0].Sum);
                    Assert.Equal(2, queryResult[1].Sum);
                    Assert.Equal(3, queryResult[2].Sum);
                }
            }
        }

        [Fact]
        public void Simple_Map_Reduce_With_Order_By_And_Projection2()
        {
            using (var store = GetShardedDocumentStore())
            {
                store.ExecuteIndex(new UserMapReduce());

                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "Grisha", Count = 1 }, "users/1");
                    newSession.Store(new User { Name = "Igal", Count = 2 }, "users/2");
                    newSession.Store(new User { Name = "Egor", Count = 3 }, "users/3");
                    newSession.SaveChanges();

                    Thread.Sleep(5000);

                    var queryResult = (from user in newSession.Query<UserMapReduce.Result, UserMapReduce>()
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

        private class AgeResult
        {
            public int Age { get; set; }
        }

        public class Dog
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Breed { get; set; }
            public string Color { get; set; }
            public int Age { get; set; }
            public bool IsVaccinated { get; set; }
        }

        public class DogsIndex : AbstractIndexCreationTask<Dog>
        {
            public class Result
            {
                public string Name { get; set; }
                public int Age { get; set; }
                public bool IsVaccinated { get; set; }
            }

            public DogsIndex()
            {
                Map = dogs => from dog in dogs
                              select new
                              {
                                  dog.Name,
                                  dog.Age,
                                  dog.IsVaccinated
                              };
            }
        }

        public class UserMapIndex : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public string Name;
            }

            public UserMapIndex()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        Name = user.Name
                    };
            }
        }

        public class UserMapReduce : AbstractIndexCreationTask<User, UserMapReduce.Result>
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
    }
}
