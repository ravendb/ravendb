//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithRemoteServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Querying
{
	public class UsingDynamicQueryWithRemoteServer : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore documentStore;

		public UsingDynamicQueryWithRemoteServer()
		{
			const int port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

			ravenDbServer = GetNewServer(port, path);
			documentStore = new DocumentStore {Url = "http://localhost:" + port}.Initialize();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		[Fact]
		public void CanPerformDynamicQueryUsingClientLinqQuery()
		{
			var blogOne = new Blog
			              	{
			              		Title = "one",
			              		Category = "Ravens"
			              	};
			var blogTwo = new Blog
			              	{
			              		Title = "two",
			              		Category = "Rhinos"
			              	};
			var blogThree = new Blog
			                	{
			                		Title = "three",
			                		Category = "Rhinos"
			                	};

			using (var s = documentStore.OpenSession())
			{
				s.Store(blogOne);
				s.Store(blogTwo);
				s.Store(blogThree);
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var results = s.Query<Blog>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
					.ToArray();

				var blogs = s.Advanced.LuceneQuery<Blog>()
					.Where("Category:Rhinos AND Title.Length:3")
					.ToArray();

				Assert.Equal(1, results.Length);
				Assert.Equal("two", results[0].Title);
				Assert.Equal("Rhinos", results[0].Category);
			}
		}

		[Fact]
		public void CanPerformDynamicQueryUsingClientLuceneQuery()
		{
			var blogOne = new Blog
			              	{
			              		Title = "one",
			              		Category = "Ravens"
			              	};
			var blogTwo = new Blog
			              	{
			              		Title = "two",
			              		Category = "Rhinos"
			              	};
			var blogThree = new Blog
			                	{
			                		Title = "three",
			                		Category = "Rhinos"
			                	};

			using (var s = documentStore.OpenSession())
			{
				s.Store(blogOne);
				s.Store(blogTwo);
				s.Store(blogThree);
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var results = s.Advanced.LuceneQuery<Blog>()
					.Where("Title.Length:3 AND Category:Rhinos")
					.WaitForNonStaleResultsAsOfNow().ToArray();

				Assert.Equal(1, results.Length);
				Assert.Equal("two", results[0].Title);
				Assert.Equal("Rhinos", results[0].Category);
			}
		}

		[Fact]
		public void CanPerformProjectionUsingClientLinqQuery()
		{
			var blogOne = new Blog
			              	{
			              		Title = "one",
			              		Category = "Ravens",
			              		Tags = new Tag[]
			              		       	{
			              		       		new Tag() {Name = "tagOne"},
			              		       		new Tag() {Name = "tagTwo"}
			              		       	}
			              	};

			using (var s = documentStore.OpenSession())
			{
				s.Store(blogOne);
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var results = s.Query<Blog>()
					.Where(x => x.Title == "one" && x.Tags.Any(y => y.Name == "tagTwo"))
					.Select(x => new
					             	{
					             		x.Category,
					             		x.Title
					             	})
					.Single();

				Assert.Equal("one", results.Title);
				Assert.Equal("Ravens", results.Category);
			}
		}

		[Fact]
		public void QueryForASpecificTypeDoesNotBringBackOtherTypes()
		{
			using (var s = documentStore.OpenSession())
			{
				s.Store(new Tag());
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var results = s.Query<Blog>()
					.Select(b => new {b.Category})
					.ToArray();
				Assert.Equal(0, results.Length);
			}
		}

		[Fact]
		public void CanPerformLinqOrderByOnNumericField()
		{
			var blogOne = new Blog
			              	{
			              		SortWeight = 2
			              	};

			var blogTwo = new Blog
			              	{
			              		SortWeight = 4
			              	};

			var blogThree = new Blog
			                	{
			                		SortWeight = 1
			                	};

			using (var s = documentStore.OpenSession())
			{
				s.Store(blogOne);
				s.Store(blogTwo);
				s.Store(blogThree);
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var resultDescending = (from blog in s.Query<Blog>()
				                        orderby blog.SortWeight descending
				                        select blog).ToArray();

				var resultAscending = (from blog in s.Query<Blog>()
				                       orderby blog.SortWeight ascending
				                       select blog).ToArray();

				Assert.Equal(4, resultDescending[0].SortWeight);
				Assert.Equal(2, resultDescending[1].SortWeight);
				Assert.Equal(1, resultDescending[2].SortWeight);

				Assert.Equal(1, resultAscending[0].SortWeight);
				Assert.Equal(2, resultAscending[1].SortWeight);
				Assert.Equal(4, resultAscending[2].SortWeight);

			}
		}

		[Fact]
		public void CanPerformLinqOrderByOnTextField()
		{
			var blogOne = new Blog
			              	{
			              		Title = "aaaaa"
			              	};

			var blogTwo = new Blog
			              	{
			              		Title = "ccccc"
			              	};

			var blogThree = new Blog
			                	{
			                		Title = "bbbbb"
			                	};

			using (var s = documentStore.OpenSession())
			{
				s.Store(blogOne);
				s.Store(blogTwo);
				s.Store(blogThree);
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var resultDescending = (from blog in s.Query<Blog>()
				                        orderby blog.Title descending
				                        select blog).ToArray();

				var resultAscending = (from blog in s.Query<Blog>()
				                       orderby blog.Title ascending
				                       select blog).ToArray();

				Assert.Equal("ccccc", resultDescending[0].Title);
				Assert.Equal("bbbbb", resultDescending[1].Title);
				Assert.Equal("aaaaa", resultDescending[2].Title);

				Assert.Equal("aaaaa", resultAscending[0].Title);
				Assert.Equal("bbbbb", resultAscending[1].Title);
				Assert.Equal("ccccc", resultAscending[2].Title);

			}
		}

		public class Blog
		{
			public User User { get; set; }

			public string Title { get; set; }

			public Tag[] Tags { get; set; }

			public int SortWeight { get; set; }

			public string Category { get; set; }
		}

		public class Tag
		{
			public string Name { get; set; }
		}

		public class User
		{
			public string Name { get; set; }
		}
	}
}