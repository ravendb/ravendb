using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class IndexTest : RavenTestBase
    {
        public IndexTest(ITestOutputHelper output) : base(output)
        {
        }


        private class User
        {
            public string Name;

            public bool Banned;
        }


        private class AllowedUsers : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "AllowedUsers";

            public AllowedUsers()
            {
                Map = users => from user in users
                               where user.Banned == false
                               select new
                               {
                                   user.Name
                               };
            }
        }


        [Fact]
        public async Task AvoidIndexWriterRecreation()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new AllowedUsers());

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var index = database.IndexStore.GetIndex("AllowedUsers");
                Assert.NotNull(index);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "karmel",
                        Banned = false
                    }, "foo/bar");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                Assert.True(index.IndexPersistence.HasWriter);

                index.IndexPersistence.Clean(IndexCleanup.All);
                await WaitAndAssertForValueAsync(() => index.IndexPersistence.HasWriter, false);

                using (var session = store.OpenSession())
                {
                    var allowed = session.Query<User, AllowedUsers>().Single();
                    Assert.Equal("karmel", allowed.Name);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "karmel",
                        Banned = true
                    }, "foo/bar");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                Assert.True(index.IndexPersistence.HasWriter);

                using (var session = store.OpenSession())
                {
                    var allowed = session.Query<User, AllowedUsers>().ToList();
                    Assert.Empty(allowed);
                }

                index.IndexPersistence.Clean(IndexCleanup.All);
                await WaitAndAssertForValueAsync(() => index.IndexPersistence.HasWriter, false);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<User, AllowedUsers>().ToList();
                    Assert.Empty(person);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "karmel",
                        Banned = true
                    }, "foo/bar/2");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                Assert.True(SpinWait.SpinUntil(() => index.IndexPersistence.HasWriter == false, TimeSpan.FromSeconds(15)));

                using (var session = store.OpenSession())
                {
                    var allowed = session.Query<User, AllowedUsers>().ToList();
                    Assert.Empty(allowed);
                }
            }
        }

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

                Indexes.WaitForIndexing(store);
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
