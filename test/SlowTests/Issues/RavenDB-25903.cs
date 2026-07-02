using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25903 : RavenTestBase
    {
        public RavenDB_25903(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldCacheTimeSeriesAndGoToServer()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today.EnsureUtc();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 500_000; i++)
                {
                    tsf.Append(baseline.AddMilliseconds(i), 1);
                }

                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(500_000, tse.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task NoCachedTimeSeriesShouldReturnNull()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today.EnsureUtc();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();

                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Null(tse);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldMergeCachedAndFetchedTimeSeriesIntoSingleRange()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today.EnsureUtc();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();

                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddMinutes(i), 1);
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                for (int i = 11; i <= 12; i++)
                {
                    tsf.Append(baseline.AddMinutes(i), 1);
                }

                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(12, tse.Length);

                Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                Assert.Equal(1, ranges.Count);
                Assert.Equal(ranges[0].From, DateTime.MinValue);
                Assert.Equal(ranges[0].To, DateTime.MaxValue);
                
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                for (int i = 11; i <= 12; i++)
                {
                    tsf.Append(baseline.AddMinutes(i).AddSeconds(30), 1);
                }

                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(14, tse.Length);
                Assert.True(IsOrdered(tse));

                Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                Assert.Equal(1, ranges.Count);
                Assert.Equal(ranges[0].From, DateTime.MinValue);
                Assert.Equal(ranges[0].To, DateTime.MaxValue);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                tsf.Append(baseline.AddMinutes(-5), 1);
                tsf.Append(baseline.AddMinutes(-3), 1);
                tsf.Append(baseline.AddMinutes(1).AddSeconds(30), 1);

                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline);
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(15, tse.Length);
                Assert.True(IsOrdered(tse));

                Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                Assert.Equal(2, ranges.Count);

                Assert.Equal(ranges[0].From, baseline.AddMinutes(-5));
                Assert.Equal(ranges[1].From, baseline);
                Assert.Equal(ranges[1].To, DateTime.MaxValue);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                tsf.Append(baseline.AddMinutes(7).AddSeconds(30), 1);
                tsf.Append(baseline.AddMinutes(30), 1);

                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(DateTime.MinValue, baseline.AddMinutes(13));
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(18, tse.Length);
                Assert.True(IsOrdered(tse));

                Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                Assert.Equal(2, ranges.Count);

                Assert.Equal(ranges[0].From, DateTime.MinValue);

                await session.SaveChangesAsync();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task AppendOldTimeseriesShouldAppendOrdered()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var listIds = new List<string>() { bookId1 };
            DateTime now = DateTime.UtcNow;
            DateTime yesterday = DateTime.Today.AddDays(-1);
            DateTime twoDaysAgo = DateTime.Today.AddDays(-2);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 3);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(yesterday, 2);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                TimeSeriesEntry[] tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(true, IsOrdered(tse));

                session.TimeSeriesFor(bookId1, nameof(Book)).Append(twoDaysAgo, 2);
                var loadDocsAgain = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(true, IsOrdered(tse));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task AppendTimeseriesInMidRangeShouldMerge()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today.EnsureUtc();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 50; i++)
                {
                    tsf.Append(baseline.AddHours(10).AddMinutes(i), 1);
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(baseline.AddHours(10).AddMinutes(5).AddSeconds(30), 1);
                var requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline.AddHours(10), baseline.AddHours(11));
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(51, tse.Sum(x => x.Value));
                Assert.Equal(true, IsOrdered(tse));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task DeletedTimeseriesNotGoToServer()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today;
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(baseline, 1);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                int requestCount = session.Advanced.NumberOfRequests;
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(1, tse.Sum(x => x.Value));

                session.TimeSeriesFor(bookId1, nameof(Book)).Delete();
                requestCount = session.Advanced.NumberOfRequests;
                tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(requestCount, session.Advanced.NumberOfRequests);
                Assert.Null(tse);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task RangePartiallyInCacheShouldMerge()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var now = DateTime.UtcNow.EnsureMilliseconds();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 1);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now.AddSeconds(3), 1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>([bookId1], b => b.IncludeTimeSeries(nameof(Book), now, now.AddHours(1)));

                var requestCount = session.Advanced.NumberOfRequests;
                Assert.Equal(2, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(now.EnsureMilliseconds(), now.AddHours(1))).Sum(x => x.Value));
                Assert.Equal(requestCount, session.Advanced.NumberOfRequests);

                using (var session2 = store.OpenAsyncSession())
                {
                    session2.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 10);
                    await session2.SaveChangesAsync();
                }

                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(now, now.AddHours(1));
                Assert.Equal(2, tse.Sum(x => x.Value));
                Assert.Equal(1, tse[0].Value);
            }

            using (var session = store.OpenAsyncSession())
            {
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(now, now);
                Assert.Equal(1, tse.Length);
                Assert.Equal(10, tse[0].Value);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task TimeSeriesIsNotOverridenWhenLoadedOnceAgain()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var bookId2 = "books/2";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.StoreAsync(new Book { Id = bookId2, Title = "Book2" }, bookId2);
                await session.SaveChangesAsync();

                session.TimeSeriesFor(bookId1, nameof(Book)).Append(DateTime.UtcNow, 1);
                session.TimeSeriesFor(bookId2, nameof(Book)).Append(DateTime.Today.AddHours(17), 2);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var listIds = new List<string>() { bookId1 };
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                loadDocs[bookId1].Title = "CurrentSessionObject";
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(DateTime.UtcNow.AddSeconds(1), 24);

                Assert.Equal(25, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
                Assert.Equal("CurrentSessionObject", (await session.LoadAsync<Book>(bookId1)).Title);

                var loadedMore = await session.LoadAsync<Book>([bookId1, bookId2], b => b.IncludeTimeSeries(nameof(Book)));
                Assert.Equal("CurrentSessionObject", loadedMore[bookId1].Title);
                Assert.Equal(25, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
                Assert.Equal(2, (await session.TimeSeriesFor(bookId2, nameof(Book)).GetAsync()).Sum(x => x.Value));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldOverrideLocallyCachedEntryWithSameTimestamp()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var bookId2 = "books/2";
            var myTime = DateTime.UtcNow;
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.StoreAsync(new Book { Id = bookId2, Title = "Book2" }, bookId2);
                await session.SaveChangesAsync();

                session.TimeSeriesFor(bookId1, nameof(Book)).Append(myTime, 1);
                session.TimeSeriesFor(bookId2, nameof(Book)).Append(DateTime.Today.AddHours(17), 2);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var listIds = new List<string>() { bookId1 };
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                loadDocs[bookId1].Title = "CurrentSessionObject";
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(myTime, 24);

                Assert.Equal(24, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
                Assert.Equal("CurrentSessionObject", (await session.LoadAsync<Book>(bookId1)).Title);

                var loadedMore = await session.LoadAsync<Book>([bookId1, bookId2], b => b.IncludeTimeSeries(nameof(Book)));
                Assert.Equal("CurrentSessionObject", loadedMore[bookId1].Title);
                Assert.Equal(24, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
                Assert.Equal(2, (await session.TimeSeriesFor(bookId2, nameof(Book)).GetAsync()).Sum(x => x.Value));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldOverrideCompletelyTimeSeriesWithSameTimestamp()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var myTime = DateTime.UtcNow;
            var listIds = new List<string>() { bookId1 };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();

                session.TimeSeriesFor(bookId1, nameof(Book)).Append(myTime, 1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                Assert.Equal(1, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));

                session.TimeSeriesFor(bookId1, nameof(Book)).Append(myTime, 24);
                Assert.Equal(24, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                Assert.Equal(24, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldTrackTimeSeriesEvenIfNoTimeseriesLoaded()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var listIds = new List<string>() { bookId1 };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>(listIds);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(DateTime.UtcNow, 1);
                Assert.Equal(1, (await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync()).Sum(x => x.Value));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldTrackIncrementedTimeSeries()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var listIds = new List<string>() { bookId1 };
            var baseline = DateTime.UtcNow;
            var book = new Book { Id = bookId1, Title = "Book1" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(book, bookId1);
                var tsf = session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice");

                tsf.Increment(baseline, 59d);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice"));
                var tse = await session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").GetAsync();

                session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").Increment(baseline, new double[] { 60, 59 });

                tse = await session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").GetAsync();
                // the in-session increment accumulates onto the server value [59] -> {119, 59}, matching the server
                Assert.Equal(new double[] {119, 59}, tse[0].Values);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var tse = await session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").GetAsync();
                Assert.Equal(new double[] { 119, 59 }, tse[0].Values);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldTrackAddedIncrementalTimeSeries()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var listIds = new List<string>() { bookId1 };
            var baseline = DateTime.UtcNow;
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").Increment(baseline.AddMinutes(1), 59d);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                TimeSeriesEntry[] tse = await session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").GetAsync();
                session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").Increment(baseline.AddMinutes(2), 60d);
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice"));

                var x = await session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").GetAsync();
                Assert.Equal(2, x.Length);
                Assert.Equal(119d, x.Sum(e => e.Value));

                await session.SaveChangesAsync();
                tse = await session.IncrementalTimeSeriesFor(bookId1, Constants.Headers.IncrementalTimeSeriesPrefix + "BookPrice").GetAsync();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task DeletingTimeseriesFromCache()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            DateTime baseline = DateTime.Today.EnsureUtc();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddHours(i), 1);
                }

                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddDays(1).AddHours(i), 1);
                }
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                int requestCount = session.Advanced.NumberOfRequests;
                await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline, baseline.AddHours(11));
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);

                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(baseline.AddDays(3), baseline.AddDays(4));
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline, baseline.AddHours(11));
                Assert.Equal(requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(10, tse.Length);

                await session.SaveChangesAsync();
                requestCount++;

                tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline, baseline.AddDays(2));
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(20, tse.Length);

                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(baseline.AddHours(6), baseline.AddDays(1).AddHours(5));
                tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(10, tse.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldRemoveTimeseriesFromCache2()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            DateTime now = DateTime.UtcNow.EnsureMilliseconds();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 3);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(now);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 1);
                var myTs = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(DateTime.Today, DateTime.Today.AddDays(1));
                
                Assert.Equal(1, myTs.Length);
                Assert.Equal(1, myTs[0].Value);
                Assert.True(((InMemoryDocumentSessionOperations)session).DeletedTimeSeries.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                Assert.Equal(0, ranges.Count);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 12; i++)
                {
                    for (int j = 1; j <= 30; j++)
                    {
                        tsf.Append(now.AddHours(i).AddMinutes(j), 1);
                    }
                }

                await session.SaveChangesAsync();

                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(now.AddHours(2), now.AddHours(3));
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now.AddHours(2).AddMinutes(44), 1);

                var myTs = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();

                Assert.Equal(332, myTs.Length);
                Assert.Equal(332, myTs.Sum(x => x.Value));

                Assert.True(((InMemoryDocumentSessionOperations)session).DeletedTimeSeries.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                Assert.Equal(2, ranges.Count);

                Assert.Equal(now.AddHours(2), ranges[0].From);
                Assert.Equal(now.AddHours(2).AddMinutes(44).AddMilliseconds(-1), ranges[0].To);
                Assert.Equal(now.AddHours(2).AddMinutes(44).AddMilliseconds(1), ranges[1].From);
                Assert.Equal(now.AddHours(3), ranges[1].To);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldRemoveTimeseriesFromCache()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            DateTime now = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 3);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(now);
                var myTs = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(DateTime.Today, DateTime.Today.AddDays(1));
                Assert.Equal(0, myTs.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task DeletingSubRangeShouldNotGoToServer()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            DateTime baseline = DateTime.Today.EnsureUtc();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 12; i++)
                {
                    tsf.Append(baseline.AddHours(i), 1);
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                int requestCount = session.Advanced.NumberOfRequests;
                await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline, baseline.AddDays(1));
                Assert.Equal(++requestCount, session.Advanced.NumberOfRequests);

                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(baseline.AddHours(5), baseline.AddHours(7));
                var tse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline.AddHours(1), baseline.AddHours(12));
                Assert.Equal(requestCount, session.Advanced.NumberOfRequests);
                Assert.Equal(9, tse.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldRemoveRangedTimeseriesFromCache()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            DateTime baseline = DateTime.Today;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddHours(i), 1);
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.TimeSeriesFor(bookId1, nameof(Book)).Delete();
                var myTs = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline, baseline.AddDays(1));
                Assert.Equal(0, myTs.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldRemovePartialRangedTimeseriesFromCache()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            DateTime baseline = DateTime.Now;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));

                for (int i = 1; i <= 10; i++)
                {
                    tsf.Append(baseline.AddHours(i), 1);
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.TimeSeriesFor(bookId1, nameof(Book)).Delete(baseline, baseline.AddHours(4));
                var myTs = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                Assert.Equal(6, myTs.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldReturnValidTimeSeriesWithinRange()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var listIds = new List<string>() { bookId1 };
            DateTime now = DateTime.UtcNow;
            DateTime nowDelayed = now.AddDays(15);
            DateTime yesterday = now.AddDays(-1);
            DateTime twoDaysAgo = now.AddDays(-2);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(now, 3);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(yesterday, 2);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(nowDelayed, 1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loadDocs = await session.LoadAsync<Book>(listIds, b => b.IncludeTimeSeries(nameof(Book)));
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(twoDaysAgo, 5);

                var allTse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync();
                var val = allTse.Sum(x => x.Value);

                Assert.Equal(11, val);
                Assert.Equal(4, allTse.Length);

                var rangedTse = await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(DateTime.MinValue, now.AddDays(1));
                val = rangedTse.Sum(x => x.Value);

                Assert.Equal(10, val);
                Assert.Equal(3, rangedTse.Length);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task NoCachingSessionShouldNotTrackAppends()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";

            using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(DateTime.UtcNow, 1);

                // with NoCaching the append must not be tracked in the session's time series cache
                Assert.False(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out _));

                await session.SaveChangesAsync();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public void SyncTypedGetAfterFullDeleteShouldNotThrow()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today.EnsureUtc();

            using (var session = store.OpenSession())
            {
                session.Store(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor<HeartRateMeasure>(bookId1).Append(baseline, new HeartRateMeasure { HeartRate = 59d });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var first = session.TimeSeriesFor<HeartRateMeasure>(bookId1).Get();
                Assert.Equal(59d, first.Single().Value.HeartRate);

                session.TimeSeriesFor<HeartRateMeasure>(bookId1).Delete();

                // sync typed read (delegates through the async path) must not throw after an in-session delete
                var afterDelete = session.TimeSeriesFor<HeartRateMeasure>(bookId1).Get();
                Assert.True(afterDelete == null || afterDelete.Length == 0);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ShouldTrackTypedTimeSeriesEvenIfNoTimeseriesLoaded()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.LoadAsync<Book>(bookId1);
                session.TimeSeriesFor<HeartRateMeasure>(bookId1).Append(DateTime.UtcNow, new HeartRateMeasure { HeartRate = 59d });

                var tse = await session.TimeSeriesFor<HeartRateMeasure>(bookId1).GetAsync();
                Assert.Equal(1, tse.Length);
                Assert.Equal(59d, tse[0].Value.HeartRate);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task TypedGetAfterFullDeleteShouldNotThrow()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.Today.EnsureUtc();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.TimeSeriesFor<HeartRateMeasure>(bookId1).Append(baseline, new HeartRateMeasure { HeartRate = 59d });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                // populate the cache with a server-backed range
                var first = await session.TimeSeriesFor<HeartRateMeasure>(bookId1).GetAsync();
                Assert.Equal(59d, first.Single().Value.HeartRate);

                // delete the whole series in-session -> the covering cached range is marked IsDeleted
                session.TimeSeriesFor<HeartRateMeasure>(bookId1).Delete();

                // typed read served from cache must not throw and must reflect the delete
                var afterDelete = await session.TimeSeriesFor<HeartRateMeasure>(bookId1).GetAsync();
                Assert.True(afterDelete == null || afterDelete.Length == 0);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task IncrementalTimeSeriesCacheShouldAccumulateOntoServerValue()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var tsName = Constants.Headers.IncrementalTimeSeriesPrefix + "Views";
            var baseline = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                session.IncrementalTimeSeriesFor(bookId1, tsName).Increment(baseline, 50d);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                // load the server value (50) into the cache
                var loaded = await session.IncrementalTimeSeriesFor(bookId1, tsName).GetAsync();
                Assert.Equal(50d, loaded[0].Value);

                // an in-session increment must accumulate onto the known server value, not override it
                session.IncrementalTimeSeriesFor(bookId1, tsName).Increment(baseline, 60d);

                var inSession = await session.IncrementalTimeSeriesFor(bookId1, tsName).GetAsync();
                Assert.Equal(1, inSession.Length);
                Assert.Equal(110d, inSession[0].Value);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var afterSave = await session.IncrementalTimeSeriesFor(bookId1, tsName).GetAsync();
                Assert.Equal(1, afterSave.Length);
                Assert.Equal(110d, afterSave[0].Value);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ServerRangeContainedInLocalRangeShouldNotDuplicateCacheRange()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.UtcNow.AddHours(-24).EnsureMilliseconds();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                for (int i = 1; i <= 10; i++)
                    tsf.Append(baseline.AddHours(i), 1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                // local append creates a wide IsLocal range [baseline+1h, sessionCreatedAt(now)]
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(baseline.AddHours(1), 1);

                // this sub-window goes to the server (NotInCache ignores local ranges);
                // the returned server range is fully contained in the local range -> merge CASE 5,
                // which used to re-add the server range a second time (duplicate).
                await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline.AddHours(2), baseline.AddHours(3));

                Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));
                AssertNoDuplicateRanges(ranges);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        public async Task ServerRangeContainedInLocalRangeShouldKeepEntriesWithinTheirRange()
        {
            using var store = GetDocumentStore();
            var bookId1 = "books/1";
            var baseline = DateTime.UtcNow.AddHours(-24).EnsureMilliseconds();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Book { Id = bookId1, Title = "Book1" }, bookId1);
                var tsf = session.TimeSeriesFor(bookId1, nameof(Book));
                for (int i = 1; i <= 10; i++)
                    tsf.Append(baseline.AddHours(i), 1);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                // wide local range [baseline+1h, now]
                session.TimeSeriesFor(bookId1, nameof(Book)).Append(baseline.AddHours(1), 1);

                // server range [+2h,+3h] is contained in the local range -> merge CASE 5 splits it
                await session.TimeSeriesFor(bookId1, nameof(Book)).GetAsync(baseline.AddHours(2), baseline.AddHours(3));

                Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue(bookId1, out var cache));
                Assert.True(cache.TryGetValue(nameof(Book), out var ranges));

                // every cached entry must lie within its own range's [From, To]
                // (previously CASE 5 let the server range absorb out-of-range entries)
                foreach (var range in ranges)
                {
                    foreach (var e in range.Entries)
                    {
                        Assert.True(e.Timestamp >= range.From && e.Timestamp <= range.To,
                            $"entry {e.Timestamp:O} escapes its range [{range.From:O}..{range.To:O}]");
                    }
                }
            }
        }

        private static void AssertNoDuplicateRanges(List<TimeSeriesRangeResult> ranges)
        {
            for (int i = 0; i < ranges.Count; i++)
            {
                for (int j = i + 1; j < ranges.Count; j++)
                {
                    Assert.False(ranges[i].From == ranges[j].From && ranges[i].To == ranges[j].To,
                        $"duplicate cached range [{ranges[i].From:O}..{ranges[i].To:O}] at indexes {i} and {j}");
                }
            }
        }

        public bool IsOrdered(TimeSeriesEntry[] entries)
        {
            for (int i = 1; i < entries.Length; i++)
            {
                if (entries[i - 1].Timestamp > entries[i].Timestamp)
                    return false;
            }
            return true;
        }

        private struct HeartRateMeasure
        {
            [TimeSeriesValue(0)] public double HeartRate;
        }

        private class Book
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }
    }
}
