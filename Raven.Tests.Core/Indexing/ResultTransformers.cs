// -----------------------------------------------------------------------
//  <copyright file="ResultTransformers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Raven.Tests.Core.Utils.Transformers;
using Xunit;

namespace Raven.Tests.Core.Indexing
{
	public class ResultTransformers : RavenCoreTestBase
	{
		[Fact]
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

		[Fact]
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

					WaitForIndexing(store);

					var results =
						session.Query<Company>()
							.TransformWith<CompanyFullAddressTransformer, CompanyFullAddressTransformer.Result>()
							.ToList().OrderByDescending(x => x.FullAddress.Length).ToArray();

					Assert.Equal("221 B Baker St, London, England", results[0].FullAddress);
					Assert.Equal("Buckingham Palace, London", results[1].FullAddress);
					Assert.Equal(string.Empty, results[2].FullAddress);
				}
			}
		}

		[Fact]
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
		public void CanSpecifyQueryInputAndIncludeInTransformer()
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
							.AddQueryInput("customData", "RavenDB Developer")
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
							.Customize(x => x.WaitForNonStaleResults())
							.TransformWith<PostWithAsDocumentTransformer, PostWithAsDocumentTransformer.Result>()
							.First();

					Assert.NotNull(post.RawDocument);
					Assert.Equal("posts/1", post.RawDocument.Value<string>(Constants.DocumentIdFieldName));
				}
			}
		}
	}
}