// -----------------------------------------------------------------------
//  <copyright file="ResultTransformers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using FastTests;

using Raven.Abstractions.Data;
using Raven.Client.Linq;
using Raven.Json.Linq;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using SlowTests.Core.Utils.Transformers;

using Xunit;

using Address = SlowTests.Core.Utils.Entities.Address;
using Company = SlowTests.Core.Utils.Entities.Company;
using Post = SlowTests.Core.Utils.Entities.Post;
using PostContent = SlowTests.Core.Utils.Entities.PostContent;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Indexing
{
    public class ResultTransformers : RavenTestBase
    {
        [Fact]
        public void BasicTransformer()
        {
            using (var store = GetDocumentStore())
            {
                var transformer1 = new Companies_NameTransformer();
                transformer1.Execute(store);

                var transformerDefinition = transformer1.CreateTransformerDefinition();
                var serverDefinition = store.DatabaseCommands.GetTransformer(transformer1.TransformerName);

                Assert.True(transformerDefinition.Equals(serverDefinition));

                var transformer2 = new Companies_ContactsTransformer();
                transformer2.Execute(store);

                transformerDefinition = transformer2.CreateTransformerDefinition();
                serverDefinition = store.DatabaseCommands.GetTransformer(transformer2.TransformerName);

                Assert.True(transformerDefinition.Equals(serverDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Amazing",
                        Type = Company.CompanyType.Public,
                        Address1 = "221 B Baker St",
                        Address2 = "London",
                        Address3 = "England",
                        Contacts = new List<Contact>
                        {
                            new Contact { Email = "email1@email.com" },
                            new Contact { Email = "email2@email.com" }
                        }
                    });

                    session.Store(new Company
                    {
                        Name = "Brilliant",
                        Type = Company.CompanyType.Public,
                        Address1 = "Buckingham Palace",
                        Address2 = "London",
                        Contacts = new List<Contact>
                        {
                            new Contact { Email = "email3@email.com" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Load<Companies_NameTransformer.Result>("companies/1", typeof(Companies_NameTransformer));

                    Assert.Equal("Amazing", result.Name);

                    var results = session.Load<Companies_ContactsTransformer.Result[]>("companies/1", typeof(Companies_ContactsTransformer));

                    Assert.Equal(2, results.Length);
                    Assert.Equal("email1@email.com", results[0].Email);
                    Assert.Equal("email2@email.com", results[1].Email);
                }

                using (var session = store.OpenSession())
                {
                    var results1 = session.Load<Companies_NameTransformer.Result>(new[] { "companies/1", "companies/2" }, typeof(Companies_NameTransformer));

                    Assert.Equal(2, results1.Length);
                    Assert.Equal("Amazing", results1[0].Name);
                    Assert.Equal("Brilliant", results1[1].Name);

                    var results2 = session.Load<Companies_ContactsTransformer.Result[]>(new[] { "companies/1", "companies/2" }, typeof(Companies_ContactsTransformer));

                    Assert.Equal(2, results2.Length);

                    Assert.Equal(2, results2[0].Length);
                    Assert.Equal("email1@email.com", results2[0][0].Email);
                    Assert.Equal("email2@email.com", results2[0][1].Email);

                    Assert.Equal(1, results2[1].Length);
                    Assert.Equal("email3@email.com", results2[1][0].Email);
                }
            }
        }

        [Fact]
        public void BasicTransformerWithLoadDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new CompanyEmployeesTransformer();
                transformer.Execute(store);

                var transformerDefinition = transformer.CreateTransformerDefinition();
                var serverDefinition = store.DatabaseCommands.GetTransformer(transformer.TransformerName);

                Assert.True(transformerDefinition.Equals(serverDefinition));

                using (var session = store.OpenSession())
                {
                    var employee1 = new Employee
                    {
                        LastName = "John"
                    };

                    var employee2 = new Employee
                    {
                        LastName = "Bob"
                    };

                    session.Store(employee1);
                    session.Store(employee2);

                    session.Store(new Company
                    {
                        Name = "Amazing",
                        EmployeesIds = new List<string> { employee1.Id, employee2.Id }
                    });

                    session.Store(new Company
                    {
                        Name = "Brilliant",
                        EmployeesIds = new List<string> { employee2.Id }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Load<CompanyEmployeesTransformer.Result>("companies/1", typeof(CompanyEmployeesTransformer));

                    Assert.Equal("Amazing", result.Name);
                    Assert.True(result.Employees.SequenceEqual(new[] { "John", "Bob" }));

                    var results = session.Load<CompanyEmployeesTransformer.Result>(new[] { "companies/1", "companies/2" }, typeof(CompanyEmployeesTransformer));

                    Assert.Equal(2, results.Length);

                    Assert.True(results[0].Employees.SequenceEqual(new[] { "John", "Bob" }));
                    Assert.Equal("Amazing", results[0].Name);

                    Assert.True(results[1].Employees.SequenceEqual(new[] { "Bob" }));
                    Assert.Equal("Brilliant", results[1].Name);
                }
            }
        }

        [Fact(Skip = "Missing feature: Collation and https://github.com/dotnet/roslyn/issues/12045")]
        public void CanApplyTransformerOnQueryResults()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_SortByName().Execute(store);
                new CompanyFullAddressTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Amazing",
                        Type = Company.CompanyType.Public,
                        Address1 = "221 B Baker St",
                        Address2 = "London",
                        Address3 = "England"
                    });

                    session.Store(new Company
                    {
                        Name = "Brilliant",
                        Type = Company.CompanyType.Public,
                        Address1 = "Buckingham Palace",
                        Address2 = "London"
                    });

                    session.Store(new Company
                    {
                        Name = "Wonderful",
                        Type = Company.CompanyType.Public,
                    });

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var results =
                        session.Query<Company, Companies_SortByName>()
                            .TransformWith<CompanyFullAddressTransformer, CompanyFullAddressTransformer.Result>()
                            .ToList().OrderByDescending(x => x.FullAddress.Length).ToArray();

                    Assert.Equal("221 B Baker St, London, England", results[0].FullAddress);
                    Assert.Equal("Buckingham Palace, London", results[1].FullAddress);
                    Assert.Equal(string.Empty, results[2].FullAddress);
                }
            }
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanApplyTransformerOnDynamicQueryResults()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new CompanyFullAddressTransformer());

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Amazing",
                        Type = Company.CompanyType.Public,
                        Address1 = "221 B Baker St",
                        Address2 = "London",
                        Address3 = "England"
                    });

                    session.Store(new Company
                    {
                        Name = "Brilliant",
                        Type = Company.CompanyType.Public,
                        Address1 = "Buckingham Palace",
                        Address2 = "London"
                    });

                    session.Store(new Company
                    {
                        Name = "Wonderful",
                        Type = Company.CompanyType.Public,
                    });

                    session.SaveChanges();

                    var results =
                        session.Query<Company>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .TransformWith<CompanyFullAddressTransformer, CompanyFullAddressTransformer.Result>()
                            .ToList().OrderByDescending(x => x.FullAddress.Length).ToArray();

                    Assert.Equal("221 B Baker St, London, England", results[0].FullAddress);
                    Assert.Equal("Buckingham Palace, London", results[1].FullAddress);
                    Assert.Equal(string.Empty, results[2].FullAddress);
                }
            }
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanLoadDocumentInTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new Posts_ByContent().Execute(store);
                new PostWithContentTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Id = "posts/1"
                    });

                    session.Store(new PostContent
                    {
                        Id = "posts/1/content",
                        Text = "Lorem ipsum..."
                    });

                    session.SaveChanges();
                    WaitForIndexing(store);

                    var queryResult = session.Query<Post, Posts_ByContent>().TransformWith<PostWithContentTransformer, PostWithContentTransformer.Result>().First();

                    Assert.Equal("Lorem ipsum...", queryResult.Content);

                    var documentQueryResult =
                        session.Advanced.DocumentQuery<PostWithContentTransformer.Result, Posts_ByContent>()
                            .SetResultTransformer("PostWithContentTransformer")
                            .First();

                    Assert.Equal("Lorem ipsum...", documentQueryResult.Content);
                }
            }
        }

        [Fact]
        public void CanSpecifyTransformerParameterAndIncludeInTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);
                new UserWithCustomDataAndAddressIncludeTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Address
                    {
                        City = "Torun",
                        Country = "Poland"
                    });

                    session.Store(new User()
                    {
                        Name = "Arek",
                        AddressId = "addresses/1"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Query<User, Users_ByName>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .TransformWith<UserWithCustomDataAndAddressIncludeTransformer, UserWithCustomDataAndAddressIncludeTransformer.Result>()
                            .AddTransformerParameter("customData", "RavenDB Developer")
                            .First();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal("RavenDB Developer", user.CustomData);

                    var address = session.Load<Address>(user.AddressId);
                    Assert.NotNull(address);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseMetadataForInTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new PostWithMetadataForTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Id = "posts/1",
                        Title = "Result Transformers"
                    });

                    session.SaveChanges();

                    var post =
                        session.Query<Post>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .TransformWith<PostWithMetadataForTransformer, PostWithMetadataForTransformer.Result>()
                            .First();

                    Assert.NotNull(post.LastModified);
                }
            }
        }

        [Fact]
        public void CanUseAsDocumentInTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new PostWithAsDocumentTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Id = "posts/1",
                        Title = "Result Transformers"
                    });

                    session.SaveChanges();

                    var post =
                        session.Query<Post>()
                            .Where(x => x.Title == "Result Transformers")
                            .Customize(x => x.WaitForNonStaleResults())
                            .TransformWith<PostWithAsDocumentTransformer, PostWithAsDocumentTransformer.Result>()
                            .First();

                    Assert.NotNull(post.RawDocument);
                    var metadata = (RavenJObject)post.RawDocument[Constants.Metadata.Key];
                    Assert.Equal("posts/1", metadata.Value<string>(Constants.Metadata.Id));
                }
            }
        }

        [Fact]
        public void CanUseTransformerWithParameterOrDefault()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_CountByLastName();
                index.Execute(store);
                var transformer = new UsersTransformer();
                transformer.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Daniel", LastName = "LastName1" });
                    session.Store(new User { Name = "Daniel2", LastName = "LastName1" });
                    session.Store(new User { Name = "Daniel2", LastName = "LastName2" });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var result = session.Query<User, Users_CountByLastName>()
                        .TransformWith<UsersTransformer, UsersTransformer.Result>()
                        .ToArray();

                    Assert.Equal(3, result.Length);
                    Assert.Equal("LastName", result[0].PassedParameter);

                    result = session.Query<User, Users_CountByLastName>()
                        .TransformWith<UsersTransformer, UsersTransformer.Result>()
                        .AddTransformerParameter("Key", "SomeParameter")
                        .ToArray();

                    Assert.Equal(3, result.Length);
                    Assert.Equal("SomeParameter", result[0].PassedParameter);
                }
            }
        }
    }
}
