//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithLocalServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Querying
{
	public class UsingDynamicQueryWithLocalServer : RavenTest
	{
		[Fact]
		public void CanPerformDynamicQueryUsingClientLinqQueryWithNestedCollection()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens",
				 Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Birds" }
				 }
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos",
				Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Mammals" }
				 }
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos",
				Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Mammals" }
				 }
			};

			using(var store = this.NewDocumentStore())
			{               
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
						.Where(x => x.Tags.Any(y=>y.Name == "Birds"))
						.ToArray();

					Assert.Equal(1, results.Length);
					Assert.Equal("one", results[0].Title);
					Assert.Equal("Ravens", results[0].Category);
				}
			}
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

			using(var store = this.NewDocumentStore())
			{               
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
						.ToArray();

					Assert.Equal(1, results.Length);
					Assert.Equal("two", results[0].Title);
					Assert.Equal("Rhinos", results[0].Category);
				}
			}
		}

		[Fact]
		public void QueryForASpecificTypeDoesNotBringBackOtherTypes()
		{
			using (var store = this.NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new BlogTag());
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>()
						.Select(b=> new { b.Category})
						.ToArray();
					Assert.Equal(0, results.Length);
				}
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

			using (var store = this.NewDocumentStore())
			{       
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Advanced.LuceneQuery<Blog>()
						.Where("Title.Length:3 AND Category:Rhinos")
						.WaitForNonStaleResultsAsOfNow().ToArray();

					Assert.Equal(1, results.Length);
					Assert.Equal("two", results[0].Title);
					Assert.Equal("Rhinos", results[0].Category);
				}
			}
		}

		public class Blog
		{
			public string Title
			{
				get;
				set;
			}

			public string Category
			{
				get;
				set;
			}

			public BlogTag[] Tags
			{
				get;
				set;
			}
		}

		public class BlogTag
		{
			public string Name { get; set; }
		}
	}
}
