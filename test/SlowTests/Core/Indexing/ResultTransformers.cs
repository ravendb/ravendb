// -----------------------------------------------------------------------
//  <copyright file="ResultTransformers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;

using FastTests;

using Raven.Abstractions.Data;

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
        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanApplyTransformerOnQueryResults()
        {
            using (var store = await GetDocumentStore())
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

        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanApplyTransformerOnDynamicQueryResults()
        {
            using (var store = await GetDocumentStore())
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

        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanLoadDocumentInTransformer()
        {
            using (var store = await GetDocumentStore())
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

        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanSpecifyTransformerParameterAndIncludeInTransformer()
        {
            using (var store = await GetDocumentStore())
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

        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanUseMetadataForInTransformer()
        {
            using (var store = await GetDocumentStore())
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

        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanUseAsDocumentInTransformer()
        {
            using (var store = await GetDocumentStore())
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
                            .Customize(x => x.WaitForNonStaleResults())
                            .TransformWith<PostWithAsDocumentTransformer, PostWithAsDocumentTransformer.Result>()
                            .First();

                    Assert.NotNull(post.RawDocument);
                    Assert.Equal("posts/1", post.RawDocument.Value<string>(Constants.DocumentIdFieldName));
                }
            }
        }

        [Fact(Skip = "Missing feature: Transformers")]
        public async Task CanUseTransformerWithParameterOrDefault()
        {
            using (var store = await GetDocumentStore())
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
