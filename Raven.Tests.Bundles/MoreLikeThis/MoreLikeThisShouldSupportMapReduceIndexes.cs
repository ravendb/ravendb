using System;
using System.Collections.Specialized;
using System.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.MoreLikeThis
{
    public class MoreLikeThisShouldSupportMapReduceIndexes : RavenTest
    {
        private readonly IDocumentStore store;

        private readonly string javascriptBookId;
        private readonly string eclipseBookId;

        public MoreLikeThisShouldSupportMapReduceIndexes()
        {
            store = NewDocumentStore();
            using (var session = store.OpenSession())
            {
                var javascriptBook = new Book { Title = "Javascript: The Good Parts", Tags = new[] { "javascript" } };
                var phpBook = new Book { Title = "PHP: The Good Parts", Tags = new[] { "php" } };
                var eclipseBook = new Book { Title = "Zend Studio for Eclipse Developer's Guide" };

                session.Store(javascriptBook);
                session.Store(phpBook);
                session.Store(eclipseBook);

                javascriptBookId = javascriptBook.Id;
                eclipseBookId = eclipseBook.Id;

                session.Store(new Author { BookId = javascriptBook.Id, Name = "Douglas Crockford" });
                session.Store(new Author { BookId = phpBook.Id, Name = "Peter MacIntyre" });
                session.Store(new Author { BookId = eclipseBook.Id, Name = "Peter MacIntyre" });
                session.Store(new Book { Title = "Unrelated" });
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
                        MapGroupFields = new NameValueCollection
                        {
                            {"BookId", javascriptBookId}
                        },
                        Fields = new[] { "Text" },
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
                        MapGroupFields = new NameValueCollection
                        {
                            {"BookId", eclipseBookId},
                        },
                        Fields = new[] { "Text" },
                        MinimumTermFrequency = 1,
                        MinimumDocumentFrequency = 1
                    });

                Assert.Equal(1, list.Count());
                Assert.Contains("PHP: The Good Parts", list.Single().Text);
            }
        }

        [Fact]
        public void CanMakeDynamicDocumentQueries()
        {
            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
                    new MoreLikeThisQuery
                    {
                        Document = "{ \"Text\": \"C#: The Good Good Parts\" }",
                        Fields = new[] { "Text" },
                        MinimumTermFrequency = 1,
                        MinimumDocumentFrequency = 1
                    });

                Assert.Equal(2, list.Count());
                Assert.Contains("Javascript: The Good Parts", list.First().Text);
            }
        }

        [Fact]
        public void CanMakeDynamicDocumentWithArrayQueries()
        {
            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
                    new MoreLikeThisQuery
                    {
                        Document = "{ \"Tags\": [\"javascript\"] }",
                        Fields = new[] { "Tags" },
                        MinimumTermFrequency = 1,
                        MinimumDocumentFrequency = 1
                    });

                Assert.Equal(1, list.Count());
                Assert.Equal("javascript", list.First().Tags.First());
            }
        }

        [Fact]
        public void ThrowExceptionForDynamicDocumentQueriesWithNonSupportedDocuments()
        {
            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                Assert.Throws<ErrorResponseException>(() => session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
                    new MoreLikeThisQuery
                    {
                        Document = "{ \"ComplexObj\": { \"Something\": \"some value\" } }",
                        Fields = new[] { "ComplexObj" },
                        MinimumTermFrequency = 1,
                        MinimumDocumentFrequency = 1
                    }));
            }
        }

        private class Book
        {
            public string Id { get; set; }
            public string Title;
            public string[] Tags;
        }

        private class Author
        {
            public string BookId;
            public string Name;
        }

        private class IndexDocument
        {
#pragma warning disable 0649
            public string BookId;
            public string Text;
            public string[] Tags;
#pragma warning restore 0649
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
                                       select new IndexDocument
                                       {
                                           BookId = thing.Id,
                                           Text = thing.Title,
                                           Tags = thing.Tags
                                       });

                AddMap<Author>(opinions => from opinion in opinions
                                           select new IndexDocument
                                           {
                                               BookId = opinion.BookId,
                                               Text = opinion.Name,
                                               Tags = new string[] { }
                                           });

                Reduce = documents => from doc in documents
                                      group doc by doc.BookId
                                          into g
                                      select new IndexDocument
                                      {
                                          BookId = g.Key,
                                          Text = string.Join(" ", g.Select(d => d.Text)),
                                          Tags = g.SelectMany(x => x.Tags).ToArray()
                                      };


                Index(x => x.Text, FieldIndexing.Analyzed);
                Index(x => x.BookId, FieldIndexing.NotAnalyzed);
                Store(x => x.BookId, FieldStorage.Yes);
                Store(x => x.Text, FieldStorage.Yes);
                Store(x => x.Tags, FieldStorage.Yes);
                TermVector(x => x.Text, FieldTermVector.Yes);
            }
        }
    }
}
