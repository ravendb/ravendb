// -----------------------------------------------------------------------
//  <copyright file="LuceneScoreTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class LuceneScoreTests : RavenTestBase
    {
        private class Book
        {
            public string Id { get; set; }
            public string Text { get; set; }
            public string Author { get; set; }
            public string Publisher { get; set; }
            public string Description { get; set; }
        }

        private class BookSummary
        {
            public string Id { get; set; }
            public string Author { get; set; }
            public string Description { get; set; }
        }

        private class BooksSearch : AbstractIndexCreationTask<Book>
        {
            public BooksSearch()
            {
                Map = books =>
                from book in books
                select new
                {
                    book.Text,
                    book.Author,
                    book.Publisher,
                    book.Description,
                    book.Id
                };

                Index(x => x.Text, FieldIndexing.Analyzed);
            }
        }


        [Fact]
        public void GetLuceneScoreWhileUsingSelect()
        {
            using (var store = GetDocumentStore())
            {
                new BooksSearch().Execute(store);
                using (var session = store.OpenSession())
                {
                    var newBook = new Book
                    {
                        Id = "Book1",
                        Author = "Verne, Jules",
                        Description =
                            "With little more than courage and ingenuity, five Union prisoners escaped the siege of Richmond-by hot-air balloon. They have no idea if they'll ever see civilization again-especially when they're swept off by a raging storm to the shores of an uncharted island",
                        Publisher = "Sampson Low, Marston, Low, and Searle",
                        Text = "Definitely don't want this coming across the wire each time because it will be huge."
                    };
                    session.Store(newBook);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results =
                        session.Advanced.DocumentQuery<Book, BooksSearch>().Where("Text: wire each time").WaitForNonStaleResultsAsOfNow().
                            Select(b => new BookSummary() { Author = b.Author, Description = b.Description, Id = b.Id }).ToList();


                    Assert.Throws<NonUniqueObjectException>(() => session.Advanced.GetMetadataFor(results[0]));
                }
            }
        }

        [Fact]
        public void GetLuceneScoreWhileNotUsingSelect()
        {
            using (var store = GetDocumentStore())
            {
                new BooksSearch().Execute(store);
                using (var session = store.OpenSession())
                {
                    var newBook = new Book()
                    {
                        Id = "Book1",
                        Author = "Verne, Jules",
                        Description =
                            "With little more than courage and ingenuity, five Union prisoners escaped the siege of Richmond-by hot-air balloon. They have no idea if they'll ever see civilization again-especially when they're swept off by a raging storm to the shores of an uncharted island",
                        Publisher = "Sampson Low, Marston, Low, and Searle",
                        Text = "Definitely don't want this coming across the wire each time because it will be huge."
                    };
                    session.Store(newBook);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results =
                        session.Advanced.DocumentQuery<Book, BooksSearch>().Where("Text: wire each time").WaitForNonStaleResultsAsOfNow().
                            ToList();
                    var scores = from result in results
                                 select session.Advanced.GetMetadataFor(result).Value<string>(Constants.Metadata.IndexScore);
                    Assert.False(string.IsNullOrWhiteSpace(scores.First()));
                }
            }
        }
    }
}
