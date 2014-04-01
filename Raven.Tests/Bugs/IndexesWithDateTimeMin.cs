using System;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class IndexesWithDateTimeMinValue : RavenTest
	{
		public class Index : AbstractMultiMapIndexCreationTask<Index.ReduceResult>
		{
			public class ReduceResult
			{
				public DateTime PublicationDate { get; set; }
				public string AuthorName { get; set; }
				public string Title { get; set; }
				public string BookId { get; set; }
				public string AuthorId { get; set; }
			}

			public Index()
			{
				AddMap<Book>(books => from book in books
				                      select new
				                      {
										  book.PublicationDate,
										  book.Title,
										  book.AuthorId,
										  BookId = book.Id,
										  AuthorName = (string)null
				                      });
				AddMap<Author>(authors => from author in authors
				                          select new
				                          {
											  PublicationDate = DateTime.MinValue,
											  Title = (string)null,
											  AuthorId = author.Id,
											  AuthorName = author.Name,
											  BookId = (string)null
				                          });

				Reduce = results => from result in results
				                    group result by result.AuthorId
				                    into g
				                    select new
				                    {
										PublicationDate = g.Select(x => x.PublicationDate).OrderByDescending(x => x).FirstOrDefault(),
										Title = g.Select(x=>x.Title).Where(x=>x!=null).FirstOrDefault(),
										AuthorId = g.Key,
										AuthorName = g.Select(x => x.AuthorName).Where(x => x != null).FirstOrDefault(),
										BookId = g.Select(x=>x.BookId).Where(x=>x!=null).FirstOrDefault()
				                    };

			}
		}

		public class Book
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public DateTime PublicationDate { get; set; }
			public string AuthorId { get; set; }
		}

		public class Author
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void ShouldJustWork()
		{
			using(var store = NewDocumentStore())
			{
				new Index().Execute(store);

				using(var session = store.OpenSession())
				{
					var author = new Author
					{
						Name = "Ayende Rahien"
					};
					session.Store(author);
					session.Store(new Book
					{
						AuthorId = author.Id,
						PublicationDate = new DateTime(2010,1,28),
						Title = "DSLs in Boo: Domain Specific Languages in .NET"
					});
					session.SaveChanges();
				}

				//WaitForUserToContinueTheTest(store);

				using(var session = store.OpenSession())
				{
					var result = session.Query<Index.ReduceResult, Index>()
						.Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
						.FirstOrDefault();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.Equal(new DateTime(2010,1,28), result.PublicationDate);
					Assert.Equal("Ayende Rahien", result.AuthorName);
					Assert.Equal("DSLs in Boo: Domain Specific Languages in .NET", result.Title);
					Assert.Equal("books/1", result.BookId);
				}
			}
		}
	}
}