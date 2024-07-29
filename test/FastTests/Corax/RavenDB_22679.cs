using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenDB_22679 : RavenTestBase
{
    public RavenDB_22679(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task MultiUnaryMatchAnd(int scenario) => await (scenario switch
    {
        0 => TestAsync(Incorrect1),
        1 => TestAsync(Correct1),
        2 => TestAsync(Correct2),
        3 => TestAsync(Correct3),
        _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null)
    });

    private class ExampleIndex : AbstractIndexCreationTask<Example>
    {
        public ExampleIndex()
        {
            Map = examples =>
                from example in examples
                select new Example { AccountId = example.AccountId, QueueId = example.QueueId, IsClosed = example.IsClosed, IsResolved = example.IsResolved };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private static IEnumerable<Example> GetSamples(int accountId, int queueId)
    {
        yield return new Example
        {
            AccountId = accountId, QueueId = queueId, IsClosed = false, IsResolved = false,
        };

        yield return new Example
        {
            AccountId = accountId, QueueId = queueId, IsClosed = true, IsResolved = true,
        };

        yield return new Example
        {
            AccountId = accountId, QueueId = queueId, IsClosed = true, IsResolved = false,
        };

        yield return new Example
        {
            AccountId = accountId, QueueId = queueId, IsClosed = false, IsResolved = true,
        };
    }


    private class Example
    {
        public string Id { get; set; }

        public int AccountId { get; set; }

        public int QueueId { get; set; }

        public bool IsResolved { get; set; }

        public bool IsClosed { get; set; }
    }

    private static IEnumerable<Example> GetSamples()
    {
        int[][] source =
        [
            [1, 123],
            [1, 124],
            [2, 123],
            [2, 124],
            [3, 123],
            [3, 124]
        ];

        int id = 0;
        foreach (int[] item in source)
        {
            foreach (Example document in GetSamples(item[0], item[1]))
            {
                id++;
                document.Id = $"examples/{id}";

                yield return document;
            }
        }
    }

    private async Task TestAsync(Func<IAsyncDocumentQuery<Example>, IAsyncDocumentQuery<Example>> prepare)
    {
        using IDocumentStore store = GetDocumentStore();

        await EnsureIndexAsync(store);
        await StoreAsync(store);

        await Indexes.WaitForIndexingAsync(store);

        using IAsyncDocumentSession session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Example> query = session
            .Advanced
            .AsyncDocumentQuery<Example, ExampleIndex>();
        query = prepare(query);
        query = query.OrderBy(e => e.AccountId);
        Example[] documents = await query.ToQueryable().ToArrayAsync();
        Assert.Equal(documents.Length, 1);
    }

    private async Task StoreAsync(IDocumentStore store)
    {
        using IAsyncDocumentSession session = store.OpenAsyncSession();

        foreach (Example document in GetSamples())
        {
            await session.StoreAsync(document);
        }

        await session.SaveChangesAsync();
    }

    private async Task EnsureIndexAsync(IDocumentStore store)
    {
        await store.ExecuteIndexAsync(new ExampleIndex());
    }


    private static IAsyncDocumentQuery<Example> Incorrect1(IAsyncDocumentQuery<Example> query)
    {
        return query
            .WhereIn(e => e.QueueId, [123])
            .AndAlso()
            .WhereEquals(e => e.AccountId, 1)
            .AndAlso()
            .OpenSubclause()
            .WhereEquals(e => e.IsClosed, false)
            .AndAlso()
            .WhereEquals(e => e.IsResolved, false)
            .CloseSubclause();
    }

    private static IAsyncDocumentQuery<Example> Correct1(IAsyncDocumentQuery<Example> query)
    {
        return query
            .WhereIn(e => e.QueueId, [123])
            .AndAlso()
            .OpenSubclause()
            .WhereEquals(e => e.IsClosed, false)
            .AndAlso()
            .WhereEquals(e => e.IsResolved, false)
            .CloseSubclause()
            .AndAlso()
            .WhereEquals(e => e.AccountId, 1);
    }

    private static IAsyncDocumentQuery<Example> Correct2(IAsyncDocumentQuery<Example> query)
    {
        return query
            .OpenSubclause()
            .WhereEquals(e => e.IsClosed, false)
            .AndAlso()
            .WhereEquals(e => e.IsResolved, false)
            .CloseSubclause()
            .AndAlso()
            .WhereIn(e => e.QueueId, [123])
            .AndAlso()
            .WhereEquals(e => e.AccountId, 1);
    }

    private static IAsyncDocumentQuery<Example> Correct3(IAsyncDocumentQuery<Example> query)
    {
        return query
            .WhereIn(e => e.QueueId, [123])
            .AndAlso()
            .WhereEquals(e => e.AccountId, 1)
            .AndAlso()
            .WhereEquals(e => e.IsClosed, false)
            .AndAlso()
            .WhereEquals(e => e.IsResolved, false);
    }
}
