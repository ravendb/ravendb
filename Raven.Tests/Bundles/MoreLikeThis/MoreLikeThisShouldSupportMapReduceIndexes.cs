using System.Collections.Specialized;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bundles.MoreLikeThis
{
	public class MoreLikeThisShouldSupportMapReduceIndexes : RavenTest
	{
		private readonly IDocumentStore store;

		private readonly string javascriptBookId;
		private string phpBookId;
		private readonly string eclipseBookId;

		public MoreLikeThisShouldSupportMapReduceIndexes()
		{
			store = NewDocumentStore();
			using (var session = store.OpenSession())
			{
				var javascriptBook = new Book {Title = "Javascript: The Good Parts"};
				var phpBook = new Book {Title = "PHP: The Good Parts"};
				var eclipseBook = new Book {Title = "Zend Studio for Eclipse Developer's Guide"};

				session.Store(javascriptBook);
				session.Store(phpBook);
				session.Store(eclipseBook);

				javascriptBookId = javascriptBook.Id;
				phpBookId = phpBook.Id;
				eclipseBookId = eclipseBook.Id;

				session.Store(new Author {BookId = javascriptBook.Id, Name = "Douglas Crockford"});
				session.Store(new Author {BookId = phpBook.Id, Name = "Peter MacIntyre"});
				session.Store(new Author {BookId = eclipseBook.Id, Name = "Peter MacIntyre"});
				session.Store(new Book {Title = "Unrelated"});
				session.SaveChanges();

				new MapReduceIndex().Execute(store);

				var results = session.Query<IndexDocument, MapReduceIndex>().Customize(x => x.WaitForNonStaleResults()).Count();

				Assert.Equal(4, results);
				Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
			}
		}

		[Fact]
		public void Can_find_book_with_similar_name()
		{
			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
					new MoreLikeThisQuery
					{
						MapGroupFields = new NameValueCollection()
						{
							{"BookId", javascriptBookId}
						},
						MinimumTermFrequency = 1,
						MinimumDocumentFrequency = 1
					});

				Assert.Equal(1, list.Count());
				Assert.Contains("PHP: The Good Parts", list.Single().Text);
			}
		}


		[Fact]
		public void Can_find_book_with_similar_author()
		{
			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
					new MoreLikeThisQuery
					{
						MapGroupFields = new NameValueCollection()
						{
							{"BookId", eclipseBookId}
						},
						MinimumTermFrequency = 1,
						MinimumDocumentFrequency = 1
					});

				Assert.Equal(1, list.Count());
				Assert.Contains("PHP: The Good Parts", list.Single().Text);
			}
		}

		private class Book
		{
			public string Id { get; set; }
			public string Title;
		}

		private class Author
		{
			public string BookId;
			public string Name;
		}

		private class IndexDocument
		{
			public string BookId;
			public string Text;
		}


		private class MapReduceIndex : AbstractMultiMapIndexCreationTask<IndexDocument>
		{
			public override string IndexName
			{
				get { return "MapReduceIndex"; }
			}

			public MapReduceIndex()
			{
				AddMap<Book>(things => from thing in things
				                       select new IndexDocument()
				                       {
					                       BookId = thing.Id,
					                       Text = thing.Title
				                       });

				AddMap<Author>(opinions => from opinion in opinions
				                           select new IndexDocument()
				                           {
					                           BookId = opinion.BookId,
					                           Text = opinion.Name
				                           });

				Reduce = documents => from doc in documents
				                      group doc by doc.BookId
				                      into g
				                      select new IndexDocument()
				                      {
					                      BookId = g.Key,
					                      Text = string.Join(" ", g.Select(d => d.Text))
				                      };


				Index(x => x.Text, FieldIndexing.Analyzed);
				Index(x => x.BookId, FieldIndexing.NotAnalyzed);
			}
		}
	}
}