using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22041 : RavenTestBase
{
    public RavenDB_22041(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task ShouldWork(Options options)
    {
        using IDocumentStore store = GetDocumentStore(options);

        await store.ExecuteIndexAsync(new Index());
        await AddTestDataAsync(store);
        await Indexes.WaitForIndexingAsync(store);
        await CountResultsAsync(store);
    }

    private static async Task CountResultsAsync(IDocumentStore store)
    {
        using IAsyncDocumentSession session = store.OpenAsyncSession();

        IAsyncDocumentQuery<Index.Result> query = session
            .Advanced
            .AsyncDocumentQuery<Index.Result, Index>()
            .OpenSubclause()
            .Not.WhereIn(t => t.NumberA, [-1])
            .AndAlso()
            .WhereEquals(t => t.DeletedAt, (DateTime?)null)
            .AndAlso()
            .WhereEquals(t => t.IsClosed, false)
            .AndAlso()
            .WhereNotEquals(t => t.NumberB, (int?)null)
            .CloseSubclause();
        int count = await query.CountAsync();
        Assert.Equal(1, count);
    }

    private static async Task AddTestDataAsync(IDocumentStore store)
    {
        using IAsyncDocumentSession session = store.OpenAsyncSession();

        foreach (Dto dto in TestData())
        {
            await session.StoreAsync(dto);
        }

        await session.SaveChangesAsync();
    }

    private static IEnumerable<Dto> TestData()
    {
        yield return new Dto { NumberA = null, NumberB = null, DeletedAt = null, IsClosed = true };

        yield return new Dto { NumberA = 1, NumberB = 2, DeletedAt = null, IsClosed = false };

        yield return new Dto { NumberA = 2, NumberB = 3, DeletedAt = DateTime.UtcNow, IsClosed = true };
    }

    private class Dto
    {
        public int? NumberA { get; set; }

        public int? NumberB { get; set; }

        public DateTime? DeletedAt { get; set; }

        public bool IsClosed { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto, Index.Result>
    {
        public Index()
        {
            Map = dtos =>
                from dto in dtos
                select new Result { NumberA = dto.NumberA, NumberB = dto.NumberB, DeletedAt = dto.DeletedAt, IsClosed = dto.IsClosed };
        }


        public class Result
        {
            public int? NumberA { get; set; }

            public int? NumberB { get; set; }

            public DateTime? DeletedAt { get; set; }

            public bool IsClosed { get; set; }
        }
    }
}
