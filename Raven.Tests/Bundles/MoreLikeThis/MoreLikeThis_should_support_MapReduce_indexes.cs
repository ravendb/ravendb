using System.Collections.Specialized;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Xunit;
using MoreLikeThisQueryParameters = Raven.Abstractions.Data.MoreLikeThisQueryParameters;


namespace Raven.Bundles.Tests.MoreLikeThis
{
	public class MoreLikeThis_should_support_MapReduce_indexes : TestWithInMemoryDatabase
	{
		public string JavascriptBookId;
		public string PhpBookId;
		public string EclipseBookId;

		public MoreLikeThis_should_support_MapReduce_indexes()
		{
			using (var session = documentStore.OpenSession())
			{
				var javascriptBook = new Book() { Title = "Javascript: The Good Parts" };
				var phpBook = new Book() { Title = "PHP: The Good Parts" };
				var eclipseBook = new Book() { Title = "Zend Studio for Eclipse Developer's Guide" };

				session.Store(javascriptBook);
				session.Store(phpBook);
				session.Store(eclipseBook);
				session.SaveChanges();

				JavascriptBookId = javascriptBook.Id;
				PhpBookId = phpBook.Id;
				EclipseBookId = eclipseBook.Id;

				session.Store(new Author() { BookId = javascriptBook.Id, Name = "Douglas Crockford" });
				session.Store(new Author() { BookId = phpBook.Id, Name = "Peter MacIntyre" });
				session.Store(new Author() { BookId = eclipseBook.Id, Name = "Peter MacIntyre" });
				session.Store(new Book() { Title = "Unrelated" });
				session.SaveChanges();

				new MapReduceIndex().Execute(documentStore);

				var results = session.Query<IndexDocument, MapReduceIndex>().Customize(x => x.WaitForNonStaleResults()).Count();

				Assert.Equal(4, results);

				Assert.Empty(documentStore.DatabaseCommands.GetStatistics().Errors);
			}
		}

		[Fact]
		public void Can_find_book_with_similar_name()
		{
			using (var session = documentStore.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
					new MoreLikeThisQueryParameters
					{
						MapGroupFields = new NameValueCollection()
						{
							{"BookId", JavascriptBookId}
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
			using (var session = documentStore.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
					new MoreLikeThisQueryParameters
					{
						MapGroupFields = new NameValueCollection()
						{
							{"BookId", EclipseBookId}
						},
						MinimumTermFrequency = 1,
						MinimumDocumentFrequency = 1
					});

				Assert.Equal(1, list.Count());
				Assert.Contains("PHP: The Good Parts", list.Single().Text);
			}
		}

		public class Book
		{
			public string Id { get; set; }
			public string Title;
		}

		public class Author
		{
			public string BookId;
			public string Name;
		}

		public class IndexDocument
		{
			public string BookId;
			public string Text;
		}


		public class MapReduceIndex : AbstractMultiMapIndexCreationTask<IndexDocument>
		{
			public override string IndexName
			{
				get
				{
					return "MapReduceIndex";
				}
			}

			public MapReduceIndex()
			{
				this.AddMap<Book>(things => from thing in things
											select new IndexDocument()
												{
													BookId = thing.Id,
													Text = thing.Title
												});

				this.AddMap<Author>(opinions => from opinion in opinions
												select new IndexDocument()
													{
														BookId = opinion.BookId,
														Text = opinion.Name
													});

				this.Reduce = documents => from doc in documents
										   group doc by doc.BookId into g
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
