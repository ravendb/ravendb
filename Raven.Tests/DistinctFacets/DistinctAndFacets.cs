// -----------------------------------------------------------------------
//  <copyright file="DistinctAndFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.DistinctFacets
{
	public class DistinctAndFacets : RavenTest
	{
		public class Book
		{
			public string Author;
			public string Category;
			public Tag[] Tags;
		}

		public class Tag
		{
			public string Name;
			public bool Primary;
		}

		public class Books_Search : AbstractIndexCreationTask<Book>
		{
			public Books_Search()
			{
				Map = books =>
					from book in books
					select new
					{
						book.Category,
						book.Author,
						PrimaryTag = book.Tags.Where(x => x.Primary).Select(x => x.Name),
						SecondayTag = book.Tags.Where(x => x.Primary == false).Select(x => x.Name)
					};
				Store(x => x.Author, FieldStorage.Yes);
			}

			public class Result
			{
				public string Author;
			}
		}

		public void SetupData(IDocumentStore store)
		{
			new Books_Search().Execute(store);
			using (var session = store.OpenSession())
			{
				session.Store(new Book
				{
					Author = "authors/1",
					Category = "Databases",
					Tags = new Tag[]
					{
						new Tag {Name = "RavenDB", Primary = true},
						new Tag {Name = "NoSQL", Primary = false}
					}
				});
				session.Store(new Book
				{
					Author = "authors/1",
					Category = "Databases",
					Tags = new Tag[]
					{
						new Tag {Name = "RavenDB", Primary = false},
						new Tag {Name = "NoSQL", Primary = true}
					}
				});
				session.SaveChanges();
			}

			WaitForIndexing(store);
		}

		[Fact]
		public void CanGetDistinctResult()
		{
			using (var store = NewDocumentStore())
			{
				SetupData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.DocumentQuery<Book, Books_Search>()
						.WhereEquals("PrimaryTag", "RavenDB").Boost(4)
						.OrElse()
						.WhereEquals("SecondayTag", "RavenDB").Boost(4)
						.Distinct()
						.SelectFields<Books_Search.Result>("Author")
						.ToList();

					Assert.Equal(1, results.Count);
				}
			}
		}

		[Fact]
		public void CanGetDistinctResult_WithPaging()
		{
			using (var store = NewDocumentStore())
			{
				SetupData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.DocumentQuery<Book, Books_Search>()
						.WhereEquals("PrimaryTag", "RavenDB").Boost(4)
						.OrElse()
						.WhereEquals("SecondayTag", "RavenDB").Boost(4)
						.Distinct()
						.SelectFields<Books_Search.Result>("Author")
						.Skip(1)
						.ToList();

					Assert.Equal(0, results.Count);
				}
			}
		}

		[Fact]
		public void CanGetDistinctResult_WithFacets()
		{
			using (var store = NewDocumentStore())
			{
				SetupData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.DocumentQuery<Book, Books_Search>()
						.WhereEquals("PrimaryTag", "RavenDB").Boost(4)
						.OrElse()
						.WhereEquals("SecondayTag", "RavenDB").Boost(4)
						.Distinct()
						.SelectFields<Books_Search.Result>("Author")
						.ToFacets(new[]
						{
							new Facet
							{
								Name = "Category"
							},
						});
					Assert.Equal("databases", results.Results["Category"].Values[0].Range);
					Assert.Equal(1, results.Results["Category"].Values[0].Hits);
				}
			}
		}

		[Fact]
		public void CanGetDistinctResult_WithFacets_LazyAndCached()
		{
			using (var store = NewDocumentStore())
			{
				SetupData(store);

				using(store.AggressivelyCache())
				using (var session = store.OpenSession())
				{
					var results = session.Advanced.DocumentQuery<Book, Books_Search>()
						.WhereEquals("PrimaryTag", "RavenDB").Boost(4)
						.OrElse()
						.WhereEquals("SecondayTag", "RavenDB").Boost(4)
						.Distinct()
						.SelectFields<Books_Search.Result>("Author")
						.ToFacetsLazy(new[]
						{
							new Facet
							{
								Name = "Category"
							},
						}).Value;
					Assert.Equal("databases", results.Results["Category"].Values[0].Range);
					Assert.Equal(1, results.Results["Category"].Values[0].Hits);

					results = session.Advanced.DocumentQuery<Book, Books_Search>()
						.WhereEquals("PrimaryTag", "RavenDB").Boost(4)
						.OrElse()
						.WhereEquals("SecondayTag", "RavenDB").Boost(4)
						.Distinct()
						.SelectFields<Books_Search.Result>("Author")
						.ToFacetsLazy(new[]
						{
							new Facet
							{
								Name = "Category"
							},
						}).Value;
					Assert.Equal("databases", results.Results["Category"].Values[0].Range);
					Assert.Equal(1, results.Results["Category"].Values[0].Hits);
				}
			}
		}
	}
}