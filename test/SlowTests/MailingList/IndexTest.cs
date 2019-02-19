using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexTest : RavenTestBase
    {
        [Fact]
        public void FloatArrayIndexTest()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new RatingByCategoryIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new Book
                    {
                        Category = Categories.Fiction,
                        Name = "Book 1",
                        Ratings = new[]
                        {
                            new Rating
                            {
                                User = "User 1",
                                Rate = 1.5F,
                            },
                            new Rating
                            {
                                User = "User 2",
                                Rate = 3.5F,
                            }
                        }
                    });

                    session.Store(new Book
                    {
                        Category = Categories.Fiction,
                        Name = "Book 2",
                        Ratings = new[]
                        {
                            new Rating
                            {
                                User = "User 3",
                                Rate = 2.5F,
                            },
                            new Rating
                            {
                                User = "User 4",
                                Rate = 4.5F,
                            }
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var categories = session
                        .Query<IndexResult, RatingByCategoryIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotNull(categories);
                    Assert.Equal(1, categories.Count);

                    var books = categories[0].Books;
                    Assert.NotNull(books);
                    Assert.Equal(2, books.Length);

                    foreach (var book in books)
                    {
                        Assert.True(book.MinRating > 1F);
                        Assert.True(book.MaxRating > book.MinRating);
                        Assert.False(string.IsNullOrEmpty(book.Name));
                    }
                }
            }
        }

        private class IndexResult
        {
            public IndexBookRating[] Books { get; set; }
            public Categories Category { get; set; }
        }

        private class IndexBookRating
        {
            public float MaxRating { get; set; }
            public float MinRating { get; set; }
            public string Name { get; set; }
        }

        private class Book
        {
            public Categories Category { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
            public Rating[] Ratings { get; set; }
        }

        private enum Categories
        {
            Fiction,
            Reference,
        }

        private class Rating
        {
            public double Rate { get; set; }
            public string User { get; set; }
        }

        private class RatingByCategoryIndex : AbstractMultiMapIndexCreationTask<RatingByCategoryIndex.IndexData>
        {
            public RatingByCategoryIndex()
            {
                AddMap<Book>(books => books
                    .Select(p => new
                    {
                        p.Name,
                        p.Category,
                        Ratings = p.Ratings.Select(x => x.Rate),
                    })
                    .Select(p => new IndexData
                    {
                        Category = p.Category,
                        Books = new dynamic[]
                        {
                            new
                            {
                                p.Name,
                                MinRating = p.Ratings.Min(),
                                MaxRating = p.Ratings.Max(),
                            }
                        },
                    }));

                Reduce = results => results
                    .GroupBy(x => x.Category)
                    .Select(g => new
                    {
                        Category = g.Key,
                        Books = g
                            .SelectMany<IndexData, object>(x => x.Books)
                            .ToArray<object>(),
                    });
            }

            public class IndexData
            {
                public dynamic[] Books { get; set; }
                public Categories Category { get; set; }
            }
        }
    }
}
