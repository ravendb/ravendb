using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_25455 : ReplicationTestBase
{
    public RavenDB_25455(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Counters)]
    public async Task CounterIsNotOverridenWhenLoadedOnceAgain()
    {
        using var store = GetDocumentStore();
        var bookId1 = "books/1";
        var bookId2 = "books/2";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
            await session.StoreAsync(new Book { Id = bookId2, Title = "Book2" }, bookId2);
            await session.SaveChangesAsync();

            session.CountersFor(bookId1).Increment(nameof(Book), 1);
            session.CountersFor(bookId2).Increment(nameof(Book), 2);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var loadDocs = await session.LoadAsync<Book>([bookId1], b => b.IncludeCounter(nameof(Book)));
            loadDocs[bookId1].Title = "CurrentSessionObject";
            session.CountersFor(bookId1).Increment(nameof(Book), 24);

            Assert.Equal(25, await session.CountersFor(bookId1).GetAsync(nameof(Book)));
            Assert.Equal("CurrentSessionObject", (await session.LoadAsync<Book>(bookId1)).Title);

            var loadedMore = await session.LoadAsync<Book>([bookId1, bookId2], b => b.IncludeCounter(nameof(Book)));
            Assert.Equal("CurrentSessionObject", loadedMore[bookId1].Title);
            Assert.Equal(25, await session.CountersFor(bookId1).GetAsync(nameof(Book)));
            Assert.Equal(2, await session.CountersFor(bookId2).GetAsync(nameof(Book)));
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task CanIncludeCountersOfSameDocumentInSeparateLoads()
    {
        using var store = GetDocumentStore();
        var bookId1 = "books/1";
        var bookId2 = "books/2";

        const string counter1 = "Book1";
        const string counter2 = "Book2";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Book(), bookId1);
            await session.StoreAsync(new Book(), bookId2);

            session.CountersFor(bookId1).Increment(counter1, 10);
            session.CountersFor(bookId1).Increment(counter2, 20);

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<Book>(bookId1, b => b.IncludeCounter(counter1));
            Assert.NotNull(doc);

            var counter1Value = await session.CountersFor(bookId1).GetAsync(counter1);
            Assert.Equal(10, counter1Value);
            Assert.Equal(1, session.Advanced.NumberOfRequests);

            // document books/1 already has counters tracked in the session
            // including more counters of this document should not throw NRE

            var moreDocs = await session.LoadAsync<Book>([bookId1, bookId2], b => b.IncludeCounter(counter2));
            Assert.Equal(2, moreDocs.Count);

            Assert.Equal(20, await session.CountersFor(bookId1).GetAsync(counter2));
            Assert.Equal(null, await session.CountersFor(bookId2).GetAsync(counter2));

            Assert.Equal(2, session.Advanced.NumberOfRequests);

        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task DeletingTrackedCounterShouldReturnNullImmediately()
    {
        using var store = GetDocumentStore();
        var bookId1 = "books/1";

        const string counter = "Likes";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Book(), bookId1);
            session.CountersFor(bookId1).Increment(counter, 10);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            // track the counter in session (include -> no server request on Get)
            var doc = await session.LoadAsync<Book>(bookId1, b => b.IncludeCounter(counter));
            Assert.NotNull(doc);

            Assert.Equal(10, await session.CountersFor(bookId1).GetAsync(counter));
            Assert.Equal(1, session.Advanced.NumberOfRequests);

            // delete should update in-session cache
            session.CountersFor(bookId1).Delete(counter);

            // should return null immediately, without going to server
            Assert.Null(await session.CountersFor(bookId1).GetAsync(counter));
            Assert.Equal(1, session.Advanced.NumberOfRequests);

            // persist deletion
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            // verify deletion persisted
            Assert.Null(await session.CountersFor(bookId1).GetAsync(counter));
        }
    }

    private class Book
    {
        public string Id { get; set; }
        public string Title { get; set; }
    }
}
