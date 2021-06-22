using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Query : RavenTestBase
    {
        public Query(ITestOutputHelper output) : base(output)
        {
        }


        public class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public string Employee { get; set; }
            public DateTime OrderedAt { get; set; }
            public DateTime RequireAt { get; set; }
            public DateTime? ShippedAt { get; set; }
            public Address ShipTo { get; set; }
            public string ShipVia { get; set; }
            public decimal Freight { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        /*
  
         */
        
        [Fact]
        public void RawQuery_with_transformation_function_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Acme Inc."
                    },"Companies/1");
                    
                    session.Store(new Company
                    {
                        Name = "Evil Corp"
                    },"Companies/2");            
                    
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
                    Assert.Equal(3,rawQuery.Count);                    
                    Assert.DoesNotContain(rawQuery,item => item == null);
                    
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Acme Inc."
                    },"Companies/1");
                    
                    session.Store(new Company
                    {
                        Name = "Evil Corp"
                    },"Companies/2");            
                    
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
                    Assert.Equal(3,complexLinqQuery.Count);                    
                    Assert.DoesNotContain(complexLinqQuery,item => item == null);
                    
                    foreach (var item in complexLinqQuery)
                    {
                        Assert.True((string)item.CompanyName == "Acme Inc." || (string)item.CompanyName == "Evil Corp");                       
                    }
                }
            }
        }

        
        [Fact]
        public void Query_Simple()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "John" }, "users/1");
                    newSession.Store(new User { Name = "Jane" }, "users/2");
                    newSession.Store(new User { Name = "Tarzan" }, "users/3");
                    newSession.SaveChanges();

                    var queryResult = newSession.Query<User>()
                        .ToList();

                    Assert.Equal(queryResult.Count, 3);
                }
            }
        }

        [Fact]
        public void Query_With_Where_Clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "John" }, "users/1");
                    newSession.Store(new User { Name = "Jane" }, "users/2");
                    newSession.Store(new User { Name = "Tarzan" }, "users/3");
                    newSession.SaveChanges();

                    var queryResult = newSession.Query<User>()
                        .Where(x => x.Name.StartsWith("J"))
                        .ToList();

                    var queryResult2 = newSession.Query<User>()
                        .Where(x => x.Name.Equals("Tarzan"))
                        .ToList();

                    var queryResult3 = newSession.Query<User>()
                        .Where(x => x.Name.EndsWith("n"))
                        .ToList();

                    Assert.Equal(queryResult.Count, 2);
                    Assert.Equal(queryResult2.Count, 1);
                    Assert.Equal(queryResult3.Count, 2);
                }
            }
        }
        
        [Fact]
        public async Task QueryWithWhere_WhenUsingStringEquals_ShouldWork()
        {
            const string constStrToQuery = "Tarzan";
            string varStrToQuery = constStrToQuery;

            using var store = GetDocumentStore();
            using var session = store.OpenAsyncSession();
            
            await session.StoreAsync(new User { Name = "John" });
            await session.StoreAsync(new User { Name = "Jane" });
            await session.StoreAsync(new User { Name = varStrToQuery });
            await session.SaveChangesAsync();

            var queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => string.Equals(x.Name, varStrToQuery))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => string.Equals(x.Name, varStrToQuery, StringComparison.OrdinalIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, constStrToQuery - ExpressionType.Constant
                .Where(x => string.Equals(x.Name, constStrToQuery))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, constStrToQuery - ExpressionType.Constant
                .Where(x => string.Equals(x.Name, constStrToQuery, StringComparison.CurrentCultureIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => string.Equals(varStrToQuery, x.Name))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => string.Equals(varStrToQuery, x.Name, StringComparison.CurrentCultureIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //constStrToQuery - ExpressionType.Constant, x.Name - ExpressionType.MemberAccess, 
                .Where(x => string.Equals(constStrToQuery, x.Name))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //constStrToQuery - ExpressionType.Constant, x.Name - ExpressionType.MemberAccess, 
                .Where(x => string.Equals(constStrToQuery, x.Name, StringComparison.CurrentCultureIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //varStrToQuery - ExpressionType.MemberAccess, x.Name - ExpressionType.MemberAccess
                .Where(x => varStrToQuery.Equals(x.Name))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //varStrToQuery - ExpressionType.MemberAccess, x.Name - ExpressionType.MemberAccess
                .Where(x => varStrToQuery.Equals(x.Name, StringComparison.OrdinalIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //varStrToQuery - ExpressionType.Constant, x.Name - ExpressionType.MemberAccess
                .Where(x => constStrToQuery.Equals(x.Name))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //varStrToQuery - ExpressionType.Constant, x.Name - ExpressionType.MemberAccess
                .Where(x => constStrToQuery.Equals(x.Name, StringComparison.OrdinalIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => x.Name.Equals(varStrToQuery))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => x.Name.Equals(varStrToQuery, StringComparison.OrdinalIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, constStrToQuery - ExpressionType.Constant
                .Where(x => x.Name.Equals(constStrToQuery))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<User>()
                //x.Name - ExpressionType.MemberAccess, constStrToQuery - ExpressionType.Constant
                .Where(x => x.Name.Equals(constStrToQuery, StringComparison.CurrentCultureIgnoreCase))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
        }

        private class Test
        {
            public string[] StrList { get; set; }
        }
        
        [Fact]
        public async Task QueryWithWhere_WhenUsingStringEqualsWhitParameterExpression_ShouldWork()
        {
            const string constStrToQuery = "Tarzan";
            string varStrToQuery = constStrToQuery;

            using var store = GetDocumentStore();
            using var session = store.OpenAsyncSession();

            await session.StoreAsync(new Test {StrList = new[]{"John"}});
            await session.StoreAsync(new Test {StrList = new[]{"Jane"}});
            await session.StoreAsync(new Test {StrList = new[]{varStrToQuery}});
            await session.SaveChangesAsync();

            var queryResult = await session.Query<Test>()
                //x1 - ExpressionType.Parameter, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => x.StrList.Any(x1 => string.Equals(x1, varStrToQuery)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x1 - ExpressionType.Parameter, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => x.StrList.Any(x1 => string.Equals(x1, varStrToQuery, StringComparison.OrdinalIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x.Name - ExpressionType.Parameter, constStrToQuery - ExpressionType.Constant
                .Where(x => x.StrList.Any(x1 => string.Equals(x1, constStrToQuery)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x.Name - ExpressionType.Parameter, constStrToQuery - ExpressionType.Constant
                .Where(x => x.StrList.Any(x1 => string.Equals(x1, constStrToQuery, StringComparison.OrdinalIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //varStrToQuery - ExpressionType.Parameter, x1 - ExpressionType.MemberAccess
                .Where(x => x.StrList.Any(x1 => string.Equals(varStrToQuery, x1)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //varStrToQuery - ExpressionType.Parameter, x1 - ExpressionType.MemberAccess
                .Where(x => x.StrList.Any(x1 => string.Equals(varStrToQuery, x1, StringComparison.OrdinalIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //constStrToQuery - ExpressionType.Constant, x1 - ExpressionType.Parameter, 
                .Where(x => x.StrList.Any(x1 => string.Equals(constStrToQuery, x1)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //constStrToQuery - ExpressionType.Constant, x1 - ExpressionType.Parameter, 
                .Where(x => x.StrList.Any(x1 => string.Equals(constStrToQuery, x1, StringComparison.OrdinalIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //varStrToQuery - ExpressionType.MemberAccess, x1 - ExpressionType.Parameter
                .Where(x => x.StrList.Any(x1 => varStrToQuery.Equals(x1)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //varStrToQuery - ExpressionType.MemberAccess, x1 - ExpressionType.Parameter
                .Where(x => x.StrList.Any(x1 => varStrToQuery.Equals(x1, StringComparison.OrdinalIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //varStrToQuery - ExpressionType.Constant, x1 - ExpressionType.Parameter
                .Where(x => x.StrList.Any(x1 => constStrToQuery.Equals(x1)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //varStrToQuery - ExpressionType.Constant, x1 - ExpressionType.Parameter
                .Where(x => x.StrList.Any(x1 => constStrToQuery.Equals(x1, StringComparison.OrdinalIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x1 - ExpressionType.Parameter, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => x.StrList.Any(x1 => x1.Equals(varStrToQuery)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x1 - ExpressionType.Parameter, varStrToQuery - ExpressionType.MemberAccess
                .Where(x => x.StrList.Any(x1 => x1.Equals(varStrToQuery, StringComparison.CurrentCultureIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x1 - ExpressionType.Parameter, constStrToQuery - ExpressionType.Constant
                .Where(x => x.StrList.Any(x1 => x1.Equals(constStrToQuery)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
            
            queryResult = await session.Query<Test>()
                //x1 - ExpressionType.Parameter, constStrToQuery - ExpressionType.Constant
                .Where(x => x.StrList.Any(x1 => x1.Equals(constStrToQuery, StringComparison.CurrentCultureIgnoreCase)))
                .ToListAsync();
            Assert.Equal(queryResult.Count, 1);
        }

        [Fact]
        public async Task QueryWithWhere_WhenUsingNotSupportedExpressions_ShouldThrowNotSupported()
        {
            using var store = GetDocumentStore();
            using var session = store.OpenAsyncSession();

            const string toQuery = "John";
            await session.StoreAsync(new User { Name = toQuery });
            await session.StoreAsync(new User { Name = "Jane" });
            await session.StoreAsync(new User { Name = "Tarzan" });
            await session.SaveChangesAsync();

            await session.StoreAsync(new Test {StrList = new[]{"John"}});
            await session.StoreAsync(new Test {StrList = new[]{"Jane"}});
            await session.StoreAsync(new Test {StrList = new[]{toQuery}});
            await session.SaveChangesAsync();
            
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await session.Query<User>()
                    .Where(x => string.Equals(x.Name, x.LastName))
                    .ToListAsync();
            });
            
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await session.Query<User>()
                    .Where(x => string.Equals(x.Name, toQuery, StringComparison.Ordinal))
                    .ToListAsync();
            });
            
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await session.Query<User>()
                    .Where(x => x.Name.Equals(x.LastName))
                    .ToListAsync();
            });
            
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await session.Query<User>()
                    .Where(x => x.Name.Equals(toQuery, StringComparison.CurrentCulture))
                    .ToListAsync();
            });

            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await session.Query<Test>()
                    .Where(x => x.StrList.Any(x1 => string.Equals(x1, toQuery, StringComparison.CurrentCulture)))
                    .ToListAsync();
            });

            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await session.Query<Test>()
                    //x1 - ExpressionType.Parameter, varStrToQuery - ExpressionType.MemberAccess
                    .Where(x => x.StrList.Any(x1 => x1.Equals(toQuery, StringComparison.Ordinal)))
                    .ToListAsync();
            });
        }
        
        [Fact]
        public void Query_With_Customize()
        {
            using (var store = GetDocumentStore())
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
                    List<DogsIndex.Result> queryResult;
                    try
                    {
                        queryResult = newSession.Query<DogsIndex.Result, DogsIndex>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .OrderBy(x => x.Name, OrderingType.AlphaNumeric)
                            .Where(x => x.Age > 2)
                            .ToList();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        for (int i = 0; i < 3; i++)
                        {
                            Console.Beep();
                        }
                        Console.ReadLine();
                        throw;
                    }

                    Assert.Equal(queryResult[0].Name, "Brian");
                    Assert.Equal(queryResult[1].Name, "Django");
                    Assert.Equal(queryResult[2].Name, "Lassie");
                    Assert.Equal(queryResult[3].Name, "Snoopy");
                }
            }
        }

        [Fact]
        public void Query_Long_Request()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var longName = new string('x', 2048);
                    newSession.Store(new User { Name = longName }, "users/1");
                    newSession.SaveChanges();

                    var queryResult = newSession.Query<User>()
                        .Where(x => x.Name.Equals(longName))
                        .ToList();

                    Assert.Equal(queryResult.Count, 1);
                }
            }
        }

        [Fact]
        public void Query_By_Index()
        {
            using (var store = GetDocumentStore())
            {
                new DogsIndex().Execute(store);
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Dog { Name = "Snoopy", Breed = "Beagle", Color = "White", Age = 6, IsVaccinated = true}, "dogs/1");
                    newSession.Store(new Dog { Name = "Brian", Breed = "Labrador", Color = "White", Age = 12, IsVaccinated = false }, "dogs/2");
                    newSession.Store(new Dog { Name = "Django", Breed = "Jack Russel", Color = "Black", Age = 3, IsVaccinated = true }, "dogs/3");
                    newSession.Store(new Dog { Name = "Beethoven", Breed = "St. Bernard", Color = "Brown", Age = 1, IsVaccinated = false }, "dogs/4");
                    newSession.Store(new Dog { Name = "Scooby Doo", Breed = "Great Dane", Color = "Brown", Age = 0, IsVaccinated = false }, "dogs/5");
                    newSession.Store(new Dog { Name = "Old Yeller", Breed = "Black Mouth Cur", Color = "White", Age = 2, IsVaccinated = true }, "dogs/6");
                    newSession.Store(new Dog { Name = "Benji", Breed = "Mixed", Color = "White", Age = 0, IsVaccinated = false }, "dogs/7");
                    newSession.Store(new Dog { Name = "Lassie", Breed = "Collie", Color = "Brown", Age = 6, IsVaccinated = true }, "dogs/8");

                    newSession.SaveChanges();

                    WaitForIndexing(store);
                }
                
                using (var newSession = store.OpenSession())
                {
                    var queryResult = newSession.Query<DogsIndex.Result, DogsIndex>()
                        .Where(x => x.Age > 2 && x.IsVaccinated == false)
                        .ToList();

                    Assert.Equal(queryResult.Count, 1);
                    Assert.Equal(queryResult[0].Name, "Brian");

                    var queryResult2 = newSession.Query<DogsIndex.Result, DogsIndex>()
                        .Where(x => x.Age <= 2 && x.IsVaccinated == false)
                        .ToList();

                    Assert.Equal(queryResult2.Count, 3);

                    var list = new List<string>();
                    foreach (var dog in queryResult2)
                    {
                        list.Add(dog.Name);
                    }
                    Assert.True(list.Contains("Beethoven"));
                    Assert.True(list.Contains("Scooby Doo"));
                    Assert.True(list.Contains("Benji")); 
                }
            }
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
    }
}
