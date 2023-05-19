//----------------------------------------------------------------------
//  <copyright file="ReferencedDocuments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
//----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit.Abstractions;

using FastTests;
using NuGet.Protocol.Plugins;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Tests.Infrastructure;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using Employee = SlowTests.Core.Utils.Entities.Employee;
using Post = SlowTests.Core.Utils.Entities.Post;
using PostContent = SlowTests.Core.Utils.Entities.PostContent;

namespace SlowTests.Core.Indexing
{
    public class ReferencedDocuments : RavenTestBase
    {
        public ReferencedDocuments(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseLoadDocumentToIndexReferencedDocs(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var postsByContent = new Posts_ByContent();
                postsByContent.Execute(store);

                var companiesWithEmployees = new Companies_WithReferencedEmployees();
                companiesWithEmployees.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var postId = "posts/" + i;

                        session.Store(new Post
                        {
                            Id = $"{postId}"
                        });

                        session.Store(new PostContent
                        {
                            Id = "posts/" + i + $"/content${postId}",
                            Text = i % 2 == 0 ? "HTML 5" : "Javascript"
                        });

                        session.Store(new Employee
                        {
                            Id = "employees/" + i,
                            LastName = "Last Name " + i
                        });
                    }

                    session.Store(new Company { EmployeesIds = new List<string>() { "employees/1", "employees/2", "employees/3" } });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var html5PostsQuery = session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5");
                    var javascriptPostsQuery = session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript");

                    Assert.Equal(5, html5PostsQuery.ToList().Count);
                    Assert.Equal(5, javascriptPostsQuery.ToList().Count);


                    var companies = session.Advanced.DocumentQuery<Company>(companiesWithEmployees.IndexName)
                        .ToArray();

                    Assert.Equal(1, companies.Length);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BasicLoadDocumentsWithEnumerable(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Companies_ByEmployeeLastName().Execute(store);

                using (var session = store.OpenSession())
                {
                    var employee1 = new Employee { LastName = "Doe" };
                    var employee2 = new Employee { LastName = "Gates" };

                    session.Store(employee1);
                    session.Store(employee2);

                    var company = new Company
                    {
                        Name = "HR",
                        EmployeesIds = new List<string>
                        {
                            employee1.Id,
                            employee2.Id
                        }
                    };

                    session.Store(company);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Companies_ByEmployeeLastName.Result, Companies_ByEmployeeLastName>()
                        .Where(x => x.LastName == "Gates")
                        .OfType<Company>()
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("HR", companies[0].Name);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BasicLoadDocuments(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Users_ByCity().Execute(store);

                using (var session = store.OpenSession())
                {
                    var address1 = new Address { City = "New York" };
                    var address2 = new Address { City = "Warsaw" };

                    session.Store(address1);
                    session.Store(address2);

                    var user1 = new User
                    {
                        LastName = "Doe",
                        AddressId = address1.Id
                    };

                    var user2 = new User
                    {
                        LastName = "Nowak",
                        AddressId = address2.Id
                    };

                    session.Store(user1);
                    session.Store(user2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "New York")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Doe", users[0].LastName);

                    var count = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Count();

                    Assert.Equal(2, count);
                }

                using (var session = store.OpenSession())
                {
                    var address = session.Load<Address>("addresses/1-A");
                    address.City = "Barcelona";

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "New York")
                        .OfType<User>()
                        .ToList();
                    var address = session.Load<Address>("addresses/1-A");
                    Assert.Equal(0, users.Count);
                    
                    Indexes.WaitForIndexing(store);
                    users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "Barcelona")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Doe", users[0].LastName);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("addresses/1-A");

                    session.SaveChanges();
                }
                
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // address from LoadDocument will be null so the City value will not get into index
                    // we cannot expect to return any users here in that case
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == null)
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(0, users.Count);
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1-A");
                    user1.AddressId = "addresses/2-A";

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

          //      WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "Warsaw")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(2, users.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1-A");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BasicLoadDocuments_Casing(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps =
                    {
                        @"from user in docs.Users
                               let address1 = LoadDocument(user.AddressId, ""addresses"")
                               let address2 = LoadDocument(user.AddressId, ""Addresses"")
                               select new
                               {
                                   City = address1.City
                               }"
                    },
                    Name = "Users/ByCity"
                }}));

                using (var session = store.OpenSession())
                {
                    var address1 = new Address { City = "New York" };
                    var address2 = new Address { City = "Warsaw" };

                    session.Store(address1);
                    session.Store(address2);

                    session.Advanced.GetMetadataFor(address2)[Constants.Documents.Metadata.Collection] = "addresses";

                    var user1 = new User
                    {
                        LastName = "Doe",
                        AddressId = address1.Id
                    };

                    var user2 = new User
                    {
                        LastName = "Nowak",
                        AddressId = address2.Id
                    };

                    session.Store(user1);
                    session.Store(user2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "New York")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Doe", users[0].LastName);

                    var count = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Count();

                    Assert.Equal(2, count);
                }

                using (var session = store.OpenSession())
                {
                    var address = session.Load<Address>("addresses/1-A");
                    address.City = "Barcelona";

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "New York")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(0, users.Count);

                    users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "Barcelona")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Doe", users[0].LastName);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("addresses/1-A");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // address from LoadDocument will be null so the City value will not get into index
                    // we cannot expect to return any users here in that case
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == null)
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(0, users.Count);
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1-A");
                    user1.AddressId = "addresses/2-A";

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Query<Users_ByCity.Result, Users_ByCity>()
                        .Where(x => x.City == "Warsaw")
                        .OfType<User>()
                        .ToList();

                    Assert.Equal(2, users.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1-A");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldReindexOnReferencedDocumentChange(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var postsByContent = new Posts_ByContent();
                postsByContent.Execute(store);

                using (var session = store.OpenSession())
                {
                    PostContent last = null;
                    for (int i = 0; i < 3; i++)
                    {
                        session.Store(new Post
                        {
                            Id = "posts/" + i
                        });

                        session.Store(last = new PostContent
                        {
                            Id = "posts/" + i + "/content",
                            Text = i % 2 == 0 ? "HTML 5" : "Javascript"
                        });
                    }

                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    Assert.Equal(2, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5").ToList().Count);
                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript").ToList().Count);

                    last.Text = "JSON"; // referenced document change

                    session.Store(last);

                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5").ToList().Count);
                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript").ToList().Count);
                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "JSON").ToList().Count);

                    session.Delete(last); // referenced document delete

                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    Assert.Equal(0, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "JSON").ToList().Count);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanProceedWhenReferencedDocumentsAreMissing(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var postsByContent = new Posts_ByContent();
                postsByContent.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Post
                        {
                            Id = "posts/" + i
                        });

                        if (i % 2 == 0)
                        {
                            session.Store(new PostContent
                            {
                                Id = "posts/" + i + "/content",
                                Text = "HTML 5"
                            });
                        }
                    }

                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    Assert.Equal(5, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", null).ToList().Count);
                }
            }
        }
        
        [Theory]
        [RavenExplicitData]
        public async Task HandleReference_ShouldCompleteTheIndexing(RavenTestParameters config)
        {
            // https://issues.hibernatingrhinos.com/issue/RavenDB-14506
            const int batchSize = 128;

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = doc =>
                {
                    doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = batchSize.ToString();
                    doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                    doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                }
            }))
            {
                var index = new Companies_ByEmployeeLastName();

                using (var session = store.OpenAsyncSession())
                {
                    var employees = Enumerable.Range(0, batchSize + 1)
                        .Select(i => new Employee {Id = $"Employees/{i}"})
                        .ToArray();

                    var company = new Company
                    {
                        Name = "HR",
                        EmployeesIds = employees.Select(e => e.Id).ToList()
                    };
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    index.Execute(store);
                    Indexes.WaitForIndexing(store);

                    //Index disable
                    store.Maintenance.Send(new DisableIndexOperation(index.IndexName));

                    foreach (var employee in employees)
                    {
                        employee.FirstName = "Changed";
                        await session.StoreAsync(employee);
                    }
                    await session.SaveChangesAsync();

                    //Index enable
                    store.Maintenance.Send(new EnableIndexOperation(index.IndexName));
                    
                    //Assert
                    Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(10));
                }
            }
        }
        
        [Theory]
        [RavenExplicitData]
        public async Task HandleReferenceAndMapping_ShouldNotMissChangedReference(RavenTestParameters config)
        {
            // https://issues.hibernatingrhinos.com/issue/RavenDB-14506
            const int batchSize = 128;

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = doc =>
                {
                    doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = batchSize.ToString();
                    doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                    doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                },
                ModifyDocumentStore = localStore =>
                {
                    localStore.Conventions = new Raven.Client.Documents.Conventions.DocumentConventions {MaxNumberOfRequestsPerSession = int.MaxValue};
                }
            }))
            {
                var index = new Companies_ByEmployeeLastName();

                using (var session = store.OpenAsyncSession())
                {
                    var list = Enumerable.Range(0, batchSize + 1)
                        .Select(i =>
                        {
                            var employeeId = $"Employees/{i}";
                            return (
                                    new Company{Id = $"Companies/{i}", EmployeesIds = new List<string>{employeeId}},
                                    new Employee {Id = employeeId}
                                );
                        })
                        .ToArray();

                    var (firstCompany, _) = list[0];
                    foreach (var (company, _) in list)
                    {
                        await session.StoreAsync(company);
                    }
                    await session.SaveChangesAsync();
                    
                    var newEmployee = new Employee{Id = $"Employees/{batchSize + 1}"};
                    firstCompany.EmployeesIds.Add(newEmployee.Id);
                    await session.StoreAsync(firstCompany);
                    await session.SaveChangesAsync();
                    
                    index.Execute(store);
                    Indexes.WaitForIndexing(store);
                    
                    //Index disable
                    store.Maintenance.Send(new DisableIndexOperation(index.IndexName));
                    foreach (var (_, employee) in list)
                    {
                        await session.StoreAsync(employee);
                    }
                    await session.SaveChangesAsync();

                    await session.StoreAsync(newEmployee);
                    await session.SaveChangesAsync();
                    
                    await session.StoreAsync(newEmployee);
                    await session.SaveChangesAsync();
                    await session.StoreAsync(new Company{EmployeesIds = new List<string>{newEmployee.Id}});
                    await session.SaveChangesAsync();

                    //Index enable
                    store.Maintenance.Send(new EnableIndexOperation(index.IndexName));
                    Indexes.WaitForIndexing(store);

                    //Assert
                    var queryResult = await session
                        .Query<Companies_ByEmployeeLastName.Result, Companies_ByEmployeeLastName>()
                        .OfType<Company>()
                        .ToArrayAsync();

                    Assert.Contains(queryResult, e => e.Id == $"Companies/{batchSize}");
                }
            }
        }
    }
}
