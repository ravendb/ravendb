using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22948 : ReplicationTestBase
    {
        public RavenDB_22948(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void RavenDB_22948_Fail()
        {
            // Arrange
            using (var testDocumentStore = GetDocumentStore())
            {
                using (var rs = testDocumentStore.OpenSession())
                {
                    // Act
                    var customer = new Customer() { Name = "My Customer" };
                    rs.Store(customer);

                    var order = new Order() { CustomerId = customer.Id, TotalPrice = 100.00 };
                    rs.Store(order);
                    rs.SaveChanges();

                    var customerWithPetName = rs
                        .Query<Order>()
                        .Select(x => new CustomerWithPet()
                        {
                            Customer = rs.Load<Customer>(x.CustomerId),
                            Pet = "My Pet"
                        })
                        .SingleOrDefault();

                    // Assert
                    Assert.Equal(customer.Id, customerWithPetName.Customer.Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void RavenDB_22948_Pass()
        {
            using (var testDocumentStore = GetDocumentStore())
            {
                using (var rs = testDocumentStore.OpenSession())
                {
                    // Act
                    var customer = new Customer() { Name = "My Customer" };
                    rs.Store(customer);

                    var order = new Order() { CustomerId = customer.Id, TotalPrice = 100.00 };
                    rs.Store(order);
                    rs.SaveChanges();

                    var qryorder = rs
                        .Query<Order>()
                        .Include(o => o.CustomerId)
                        .Where(x => x.Id == order.Id)
                        .SingleOrDefault();

                    Customer customerret = rs
                        .Load<Customer>(qryorder.CustomerId);

                    var custwithpet = new CustomerWithPet()
                    {
                        Customer = customerret,
                        Pet = "My Pet"
                    };

                    // Assert
                    Assert.Equal(customer.Id, custwithpet.Customer.Id);
                }
            }
        }

        private class CustomerWithPet
        {
            public Customer Customer { get; set; }

            public string Pet { get; set; }
        }

        private class Order : EntityWithId
        {
            public string CustomerId { get; set; }

            public double TotalPrice { get; set; }
        }

        private class Customer : EntityWithId
        {

            public string Name { get; set; }
        }

        private static int IdCounter;

        private abstract class EntityWithId
        {
            public EntityWithId()
            {
                Id = GenerateId();
            }
            public string Id { get; set; }

            public virtual string GenerateId()
            {
                IdCounter++;
                return GetType().Name + "-" + IdCounter;
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void Test_TwoClients_DifferentIdHandling()
        {
            using (var defaultStore = new DocumentStore
            {
                Urls = new []{Server.WebUrl},
                Database = "Test"
            }.Initialize())
            {
                var doc1Id = "users/1";
                var doc2Id = "users/2";

                using (var customStore = GetDocumentStore(new Options
                       {
                           ModifyDatabaseName = x =>"Test",
                           ModifyDocumentStore = s =>
                           {
                               s.Conventions.FindIdentityPropertyNameFromCollectionName = (typeName) => "TestId";
                               s.Conventions.FindIdentityProperty = prop => prop.Name == "TestId";
                           }
                       }))
                {
                    //Id is identity property
                    using (var session1 = defaultStore.OpenSession())
                    {
                        var defaultUser = new User
                        {
                            Id = doc1Id,
                            TestId = "users/1-T",
                            Name = "Default Store User"
                        };
                        session1.Store(defaultUser);
                        session1.SaveChanges();
                    }

                    //TestId is identity property
                    using (var session2 = customStore.OpenSession())
                    {
                        var customUser = new User
                        {
                            Id = "users/2-T",
                            TestId = doc2Id,
                            Name = "Custom Store User"
                        };
                        session2.Store(customUser);
                        session2.SaveChanges();

                        var user1 = session2.Load<User>(doc1Id);
                        user1.Id = "users/3";
                        session2.Store(user1);
                        session2.SaveChanges();
                    }

                    //Id is identity property
                    using (var session1 = defaultStore.OpenSession())
                    {
                        var query = session1.Query<User>().Select(x => new { Id = x.Id, TestId = x.TestId });
                        var results = query.ToList();

                        Assert.Null(results[0].TestId);
                        Assert.Equal(doc2Id, results[0].Id);

                        Assert.Null(results[1].TestId);
                        Assert.Equal(doc1Id, results[1].Id);
                    }

                    //TestId is identity property
                    using (var session2 = customStore.OpenSession())
                    {
                        var query = session2.Query<User>().Select(x => new { Id = x.Id, TestId = x.TestId });
                        var results = query.ToList();

                        Assert.Equal("users/2-T", results[0].Id);
                        Assert.Equal(doc2Id, results[0].TestId);

                        Assert.Equal("users/3", results[1].Id);
                        Assert.Equal(doc1Id, results[1].TestId);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void Test_TwoClients_DifferentIdHandlingWithLoad()
        {
            using (var defaultStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = "Test"
            }.Initialize())
            {
                var doc1Id = "users/1";
                var doc2Id = "users/2";

                using (var customStore = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = x => "Test",
                    ModifyDocumentStore = s =>
                    {
                        s.Conventions.FindIdentityPropertyNameFromCollectionName = (typeName) => "TestId";
                        s.Conventions.FindIdentityProperty = prop => prop.Name == "TestId";
                    }
                }))
                {
                    //Id is identity property
                    using (var session1 = defaultStore.OpenSession())
                    {
                        var defaultUser = new User
                        {
                            Id = doc1Id,
                            TestId = "users/1-T",
                            Name = "Default Store User"
                        };
                        session1.Store(defaultUser); //Document Id is users/1
                        session1.SaveChanges();
                    }

                    //TestId is identity property
                    using (var session2 = customStore.OpenSession())
                    {
                        var customUser = new User
                        {
                            Id = "users/2-T",
                            TestId = doc2Id,
                            Name = "Custom Store User"
                        };
                        session2.Store(customUser); //Document Id is users/2
                        session2.SaveChanges();

                        var user1 = session2.Load<User>(doc1Id);
                        user1.Id = "users/3";
                        session2.Store(user1); // changing Id property users/1 document to users/3
                        session2.SaveChanges();
                    }

                    //Id is identity property
                    using (var session1 = defaultStore.OpenSession())
                    {
                        var results = session1
                            .Query<User>()
                            .Select(x => new UserWithPet()
                            {
                                User = session1.Load<User>(x.Id),
                                Pet = "My Pet"
                            })
                            .ToList();

                        Assert.Equal("users/2-T", results[0].User.Id);
                        Assert.Null(results[0].User.TestId);

                        Assert.Equal("users/3", results[1].User.Id);
                        Assert.Null(results[1].User.TestId);
                    }

                    //TestId is identity property
                    using (var session2 = customStore.OpenSession())
                    {
                        var results = session2
                            .Query<User>()
                            .Select(x => new UserWithPet()
                            {
                                User = session2.Load<User>(x.TestId),
                                Pet = "My Pet"
                            })
                            .ToList();

                        Assert.Equal("users/2-T", results[0].User.Id);
                        Assert.Equal(doc2Id, results[0].User.TestId);

                        Assert.Equal("users/3", results[1].User.Id);
                        Assert.Equal(doc1Id, results[1].User.TestId);
                    }
                }
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string TestId { get; set; }
        }

        private class UserWithPet
        {
            public User User { get; set; }

            public string Pet { get; set; }
        }
    }
}
