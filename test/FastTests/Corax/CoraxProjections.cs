using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CoraxProjections : RavenTestBase
{
    public CoraxProjections(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MapReduceIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        CreateSampleDb<MyMapReduceIndex>(store);
WaitForUserToContinueTheTest(store);
        using var session = store.OpenAsyncSession();
        var results = await session
            .Query<MyMapReduceIndex.Result, MyMapReduceIndex>()
            .OrderByDescending(i => i.Count)
            .Select(i => i.Count)
            .ToArrayAsync();

        var expected = new int[] {-1, -2, -3};
        Assert.Equal(expected, results);
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {ProjectionBehavior.FromIndex})]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {ProjectionBehavior.FromDocument})]
    public async Task MapIndex(Options options, ProjectionBehavior projectionBehavior)
    {
        using var store = GetDocumentStore(options);
        CreateSampleDb<MyMapIndex>(store);

        using var session = store.OpenAsyncSession();
        var results = await session
            .Query<Dto, MyMapIndex>()
            .Customize(x => x.Projection(projectionBehavior))
            .OrderByDescending(i => i.Count)
            .Select(i => (int?)i.Count)
            .ToArrayAsync();

        var expected = (options.SearchEngineMode, projectionBehavior) switch
        {
            (_, ProjectionBehavior.FromIndex) => new int?[] {null, null, null},
            (_, ProjectionBehavior.FromDocument) => new int?[] {1, 2, 3},
            _ => throw new ArgumentOutOfRangeException()
        };

        Assert.Equal(expected, results);
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {ProjectionBehavior.FromIndex})]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {ProjectionBehavior.FromDocument})]
    public async Task CanProjectFromIndexInAutoIndexes(Options options, ProjectionBehavior projectionBehavior)
    {
        using var store = GetDocumentStore(options);
        DeployData(store);

        using (var session = store.OpenAsyncSession())
        {
            var results = await session
                .Query<Dto>()
                .Customize(i => i.WaitForNonStaleResults())
                .Where(i => i.Name == "maciej")
                .Select(i => new {i.Id, i.Name})
                .ToListAsync();

            Assert.Equal("Maciej", results[0].Name);

            await store.Maintenance.SendAsync(new StopIndexingOperation());

            var doc = await session.LoadAsync<Dto>(results[0].Id);
            doc.Name = "Second";
            await session.StoreAsync(doc);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var results = await session
                .Query<Dto>()
                .Customize(x => x.Projection(projectionBehavior))
                .Where(i => i.Name == "maciej")
                .Select(i => i.Name)
                .ToListAsync();

            var expected = (options.SearchEngineMode, projectionBehavior) switch
            {
                (_, ProjectionBehavior.FromIndex) => null,
                (_, ProjectionBehavior.FromDocument) => "Second",
                _ => throw new ArgumentOutOfRangeException()
            };

            Assert.Equal(expected, results[0]);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {ProjectionBehavior.FromIndex})]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {ProjectionBehavior.FromDocument})]
    public async Task CanProjectFromDynamicField(Options options, ProjectionBehavior projectionBehavior)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new DtoWithDict() {Values = new Dictionary<string, string>() {{nameof(Dto.Name), "Maciej"}}, Name = "Jan"});
            await session.SaveChangesAsync();
        }

        await new DynamicIndexProjection().ExecuteAsync(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            var result = await session
                .Query<DtoWithDict, DynamicIndexProjection>()
                .Customize(x => x.Projection(projectionBehavior))
                .Select(i => i.Name)
                .SingleAsync();
                
            WaitForUserToContinueTheTest(store);
            var expected = (options.SearchEngineMode, projectionBehavior) switch
            {
                (_, ProjectionBehavior.FromDocument) => "Jan",
                (_, ProjectionBehavior.FromIndex) => "Maciej",
                _ => throw new ArgumentOutOfRangeException()
            };

            Assert.Equal(expected, result);
        }
    }

    private void CreateSampleDb<TIndex>(IDocumentStore store) where TIndex : AbstractIndexCreationTask, new()
    {
        DeployData(store);
        var index = new TIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private void DeployData(IDocumentStore store)
    {
        using (var session = store.OpenSession())
        {
            session.Store(new Dto() {Name = "Maciej", Count = 1});
            session.Store(new Dto() {Name = "Gracjan", Count = 2});
            session.Store(new Dto() {Name = "Marcin", Count = 3});
            session.SaveChanges();
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Count { get; set; }
    }

    private class DtoWithDict : Dto
    {
        public Dictionary<string, string> Values;
    }


    private class MyMapIndex : AbstractIndexCreationTask<Dto>
    {
        public MyMapIndex()
        {
            Map = dtos => dtos.Select(i => new {i.Id, i.Name, Count = i.Count * (-1)});
        }
    }

    private class DynamicIndexProjection : AbstractIndexCreationTask<DtoWithDict>
    {
        public DynamicIndexProjection()
        {
            Map = dicts => dicts.Select(i => new {_ = i.Values.Select(z => CreateField(z.Key, z.Value,true, false))});
        }
    }

    private class MyMapReduceIndex : AbstractIndexCreationTask<Dto, MyMapReduceIndex.Result>
    {
        public class Result
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public MyMapReduceIndex()
        {
            Map = dtos => dtos.Select(i => new Result() {Name = i.Name, Count = i.Count});
            Reduce = results => results.GroupBy(i => i.Name).Select(g => new Result() {Name = g.Key, Count = -1 * g.Sum(i => i.Count)});
        }
    }

    public class MyCounterIndex : AbstractCountersIndexCreationTask<Company>
    {
        public MyCounterIndex()
        {
            AddMap("HeartRate", counters => from counter in counters
                select new {HeartBeat = counter.Value, Name = counter.Name, User = counter.DocumentId});
        }
    }
}
