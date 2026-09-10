using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Lazy.Async;

public class RavenDB_25420(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string TimeSeriesName = "ts";
    private readonly DateTime _baseLine = DateTime.UtcNow;

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(20, 0, 1)] // no preload query, just the lazy load with includes
    [InlineData(20, 1, 2)] // preload 1 book, lazy load other with includes
    [InlineData(20, 2, 2)] // preload 2 books, lazy load other with includes
    [InlineData(20, 5, 2)] // preload 2 books, lazy load other with includes
    [InlineData(20, 18, 2)] // preload 18 books, lazy load other with includes
    [InlineData(20, 20, 2)] // preload all books, what happens with the lazy includes in this scenario?
    public async Task TestLazyLoadWhenSomeDocumentsMightBeAlreadyLoaded(int booksAndAuthorsCount, int preloadBooksCount, int maxExpectedRequests)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenAsyncSession())
        {
            for (var i = 0; i < booksAndAuthorsCount; i++)
            {
                var author = new Author()
                {
                    Id = $"authors/{i}",
                    Name = $"Author {i}"
                };
                var book = new Book()
                {
                    Id = $"books/{i}",
                    Title = $"Book {i}",
                    AuthorId = $"authors/{i}",
                };
                await session.StoreAsync(author);
                await session.StoreAsync(book);
            }

            await session.SaveChangesAsync();
        }

        // some books are preloaded
        using (var session = store.OpenAsyncSession())
        {
            // preload N books
            if (preloadBooksCount > 0)
            {
                await session.LoadAsync<Book>(Enumerable.Range(0, preloadBooksCount)
                    .Select(i => $"books/{i}"));
            }

            var allBooksIds = new List<string>(Enumerable.Range(0, booksAndAuthorsCount).Select(i => $"books/{i}"));
            var lazyBooks = session.Advanced.Lazily
                .Include<Book>(doc => doc.AuthorId)
                .LoadAsync<Book>(allBooksIds);

            // execute pending lazy operations
            await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

            int requestCount = session.Advanced.NumberOfRequests;
            Assert.True(requestCount <= 2); // preload + lazy load (if not already all in session)

            var books = await lazyBooks.Value;
            Assert.Equal(booksAndAuthorsCount, books.Count);
            Assert.Equal(booksAndAuthorsCount, books.Select(item => item.Value.Id).Distinct().Count());

            foreach (var pair in books)
            {
                var author = await session.LoadAsync<Author>(pair.Value.AuthorId);
                Assert.NotNull(author);
            }


            Assert.True(
                session.Advanced.NumberOfRequests <= maxExpectedRequests,
                $"Expected no more than {maxExpectedRequests} requests, got {session.Advanced.NumberOfRequests}"
            );
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadRevisionsInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out _, out _, out _, out var book1, out var book2, out var book3, out var updateTime);
        using var session = store.OpenAsyncSession();
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
        Assert.Equal(1, session.Advanced.NumberOfRequests);

        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id], builder => builder.IncludeRevisions(updateTime));

        // Should not send any additional requests
        var rv1 = await session.Advanced.Revisions.GetAsync<Book>(book1.Id, updateTime);
        var rv2 = await session.Advanced.Revisions.GetAsync<Book>(book2.Id, updateTime);
        var rv3 = await session.Advanced.Revisions.GetAsync<Book>(book3.Id, updateTime);
        Assert.EndsWith("_O", rv1.Title);
        Assert.EndsWith("_O", rv2.Title);
        Assert.EndsWith("_O", rv3.Title);
        Assert.Equal(2, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadAllTimeSeriesInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out _, out _, out _, out var book1, out var book2, out var book3, out _);
        using var session = store.OpenAsyncSession();
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
        Assert.Equal(1, session.Advanced.NumberOfRequests);

        // Should load the timeseries in bulk
        var baseLineOffset = _baseLine.AddMinutes(5);
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id], builder => builder.IncludeAllTimeSeries(_baseLine, baseLineOffset));

        var ts1 = await session.TimeSeriesFor<Book>(book1.Id, TimeSeriesName).GetAsync(_baseLine, baseLineOffset);
        Assert.Equal(1, ts1.Length);
        Assert.Equal(1, ts1[0].Values.Single());
        
        var ts2 = await session.TimeSeriesFor<Book>(book2.Id, TimeSeriesName).GetAsync(_baseLine, baseLineOffset);
        Assert.Equal(1, ts2.Length);
        Assert.Equal(2, ts2[0].Values.Single());

        var ts3 = await session.TimeSeriesFor<Book>(book3.Id, TimeSeriesName).GetAsync(_baseLine, baseLineOffset);
        Assert.Equal(1, ts3.Length);
        Assert.Equal(3, ts3[0].Values.Single());

        Assert.Equal(2, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadTimeSeriesInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out _, out _, out _, out var book1, out var book2, out var book3, out _);
        using var session = store.OpenAsyncSession();
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
        Assert.Equal(1, session.Advanced.NumberOfRequests);

        var baseLineOffset = _baseLine.AddMinutes(5);
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id], builder => builder.IncludeTimeSeries(TimeSeriesName, _baseLine, baseLineOffset));

        var ts1 = await session.TimeSeriesFor<Book>(book1.Id, TimeSeriesName).GetAsync(_baseLine, baseLineOffset);
        Assert.Equal(1, ts1.Length);
        Assert.Equal(1, ts1[0].Values.Single());
        
        var ts2 = await session.TimeSeriesFor<Book>(book2.Id, TimeSeriesName).GetAsync(_baseLine, baseLineOffset);
        Assert.Equal(1, ts2.Length);
        Assert.Equal(2, ts2[0].Values.Single());

        var ts3 = await session.TimeSeriesFor<Book>(book3.Id, TimeSeriesName).GetAsync(_baseLine, baseLineOffset);
        Assert.Equal(1, ts3.Length);
        Assert.Equal(3, ts3[0].Values.Single());

        Assert.Equal(2, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadCountersInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out _, out _, out _, out var book1, out var book2, out var book3, out _);
        using var session = store.OpenAsyncSession();

        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
        Assert.Equal(1, session.Advanced.NumberOfRequests);

        // Load counters in bulk
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id], b => b.IncludeCounter(nameof(Author)));

        var c1 = await session.CountersFor(book1.Id).GetAsync(nameof(Author));
        var c2 = await session.CountersFor(book2.Id).GetAsync(nameof(Author));
        var c3 = await session.CountersFor(book3.Id).GetAsync(nameof(Author));
        Assert.Equal(c1, 1);
        Assert.Equal(c2, 2);
        Assert.Equal(c3, 3);
        Assert.Equal(2, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadAllCountersInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out _, out _, out _, out var book1, out var book2, out var book3, out _);
        using var session = store.OpenAsyncSession();

        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
        Assert.Equal(1, session.Advanced.NumberOfRequests);

        // Load counters in bulk
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id], b => b.IncludeAllCounters());

        var c1 = await session.CountersFor(book1.Id).GetAsync(nameof(Author));
        var c2 = await session.CountersFor(book2.Id).GetAsync(nameof(Author));
        var c3 = await session.CountersFor(book3.Id).GetAsync(nameof(Author));
        Assert.Equal(c1, 1);
        Assert.Equal(c2, 2);
        Assert.Equal(c3, 3);
        Assert.Equal(2, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadCompareExchangeInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out _, out _, out _, out var book1, out var book2, out var book3, out _);
        using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });

        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
        Assert.Equal(1, session.Advanced.NumberOfRequests);

        // Load counters in bulk
        await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id], b => b.IncludeCompareExchangeValue(p => p.CompareExchangeName));

        var cmx1 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(book1.CompareExchangeName);
        Assert.Equal("1", cmx1.Value);

        var cmx2 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(book2.CompareExchangeName);
        Assert.Equal("2", cmx2.Value);

        var cmx3 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(book3.CompareExchangeName);
        Assert.Equal("3", cmx3.Value);
        
        Assert.Equal(2, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task LoadIncludesInSingleCallEvenDocumentsAreAlreadyLoaded()
    {
        using var store = GetDocumentStoreWithDocuments(out var author1, out var author2, out var author3, out var book1, out var book2, out var book3, out _);
        using (var session = store.OpenAsyncSession())
        {
            // preload all books
            await session.LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
            Assert.Equal(1, session.Advanced.NumberOfRequests);

            // load with includes
            await session.Include<Book>(x => x.AuthorId).LoadAsync<Book>([book1.Id, book2.Id, book3.Id]);
            Assert.Equal(2, session.Advanced.NumberOfRequests);

            var a1 = await session.LoadAsync<Author>(author1.Id);
            Assert.NotNull(a1);
            var a2 = await session.LoadAsync<Author>(author2.Id);
            Assert.NotNull(a2);

            var a3 = await session.LoadAsync<Author>(author3.Id);
            Assert.NotNull(a3);

            Assert.Equal(2, session.Advanced.NumberOfRequests);
        }
    }

    private DocumentStore GetDocumentStoreWithDocuments(out Author author1, out Author author2, out Author author3, out Book book1, out Book book2, out Book book3, out DateTime updateTime)
    {
        var store = GetDocumentStore();
        AsyncHelpers.RunSync(() => RevisionsHelper.SetupRevisionsAsync(store));

        using (var session = store.OpenSession())
        {
            author1 = new Author() { Name = "Author1" };
            author2 = new Author() { Name = "Author2" };
            author3 = new Author() { Name = "Author3" };
            session.Store(author1);
            session.Store(author2);
            session.Store(author3);
            session.SaveChanges();

            book1 = new Book() { AuthorId = author1.Id, Title = "Book1_O", CompareExchangeName = "Cmpx1" };
            book2 = new Book() { AuthorId = author2.Id, Title = "Book2_O", CompareExchangeName = "Cmpx2" };
            book3 = new Book() { AuthorId = author3.Id, Title = "Book3_O", CompareExchangeName = "Cmpx3" };
            session.Store(book1);
            session.Store(book2);
            session.Store(book3);
            session.SaveChanges();

            var bts1 = session.TimeSeriesFor(book1.Id, TimeSeriesName);
            bts1.Append(_baseLine.AddMinutes(1), 1D);

            var bts2 = session.TimeSeriesFor(book2.Id, TimeSeriesName);
            bts2.Append(_baseLine.AddMinutes(2), 2D);

            var bts3 = session.TimeSeriesFor(book3.Id, TimeSeriesName);
            bts3.Append(_baseLine.AddMinutes(3), 3D);

            session.CountersFor(book1.Id).Increment(nameof(Author), 1);
            session.CountersFor(book2.Id).Increment(nameof(Author), 2);
            session.CountersFor(book3.Id).Increment(nameof(Author), 3);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            updateTime = DateTime.UtcNow;
            book1 = session.Load<Book>(book1.Id);
            book2 = session.Load<Book>(book2.Id);
            book3 = session.Load<Book>(book3.Id);
            book1.Title = "Book1";
            book2.Title = "Book2";
            book3.Title = "Book3";
            session.SaveChanges();
        }

        using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            CompareExchangeValue<string> item1 =
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(
                    key: book1.CompareExchangeName,
                    value: "1"
                );
            CompareExchangeValue<string> item2 =
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(
                    key: book2.CompareExchangeName,
                    value: "2"
                );
            CompareExchangeValue<string> item3 =
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(
                    key: book3.CompareExchangeName,
                    value: "3"
                );

            session.SaveChanges();
        }

        return store;
    }

    private class Book
    {
        public string Id { get; set; }
        public string Title { get; set; }

        public string AuthorId { get; set; }

        public string CompareExchangeName { get; set; }
    }

    private class Author
    {
        public string Id { get; set; }

        public string Name { get; set; }
    }
}
