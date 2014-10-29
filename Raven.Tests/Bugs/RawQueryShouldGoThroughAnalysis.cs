// -----------------------------------------------------------------------
//  <copyright file="RawQueryShouldGoThroughAnalysis.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Xml.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class RawQueryShouldGoThroughAnalysis : RavenTest
	{
		[Fact]
		public void WillWork()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Book {Author = "Dan Brown"});
					session.SaveChanges();
				}

				store.ExecuteIndex(new BooksIndex());
				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<Book, BooksIndex>().Search(x => x.Author, "Brown").ToList());
					Assert.NotEmpty(session.Query<Book, BooksIndex>().Search(x => x.Author, "Brow*", 1, SearchOptions.Or, EscapeQueryOptions.AllowPostfixWildcard).ToList());
					Assert.NotEmpty(session.Query<Book, BooksIndex>().Search(x => x.Author, "bro?n", 1, SearchOptions.Or, EscapeQueryOptions.RawQuery).ToList());
				}
			}
		}

		[Fact]
		public void WillFail()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Book { Author = "Dan Brown" });
					session.SaveChanges();
				}

				store.ExecuteIndex(new BooksIndex());
				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<Book, BooksIndex>().Search(x => x.Author, "Bro?n", 1, SearchOptions.Or, EscapeQueryOptions.RawQuery).ToList());
				}
			}
		}
	}

	public class BooksIndex : AbstractIndexCreationTask<Book>
	{
		public BooksIndex()
		{
			Map = books => from book in books
			               select new {book.Author};

			Index(book => book.Author, FieldIndexing.Analyzed);
		}
	}

	public class Book
	{
		public string Author { get; set; }
	}
}