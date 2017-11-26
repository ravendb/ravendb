using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class Query : RavenTestBase
    {

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

