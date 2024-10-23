using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class RavenDB_22999(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public async Task CaseInsensitiveStringComparisonTest()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                var book1 = new Book()
                {
                    Id = "books/1",
                    Title = "Book1",
                };
                var book2 = new Book()
                {
                    Id = "books/2",
                    Title = "Book2 ČĐŽŠĆ",
                };

                await session.StoreAsync(book1, string.Empty, book1.Id);
                await session.StoreAsync(book2, string.Empty, book2.Id);

                await session.SaveChangesAsync();
            }

            // 1) GOOD: ASCII characters comparison not matching the casing
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "book1")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book1", books.First().Title);
            }

            // 2) GOOD: UTF8 characters comparison matching the casing
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "Book2 ČĐŽŠĆ")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book2 ČĐŽŠĆ", books.First().Title);
            }

            // 3) GOOD: UTF8 characters comparison ONLY ASCII CHARACTER DIFFERENT CASING
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "BOOK2 ČĐŽŠĆ")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book2 ČĐŽŠĆ", books.First().Title);
            }

            // 4) FAILING: UTF8 characters comparison NON-ASCII CHARACTER DIFFERENT CASING
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "Book2 čĐŽŠĆ")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book2 ČĐŽŠĆ", books.First().Title);
            }
        }

        public class Book
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }
    }
}
