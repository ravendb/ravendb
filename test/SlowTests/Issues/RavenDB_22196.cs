using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22196 : RavenTestBase
{
    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(TestMatrix))]
    public void SingleCollectionLoadTest(bool disableIndex, bool turnOffDatabase)
    {
        using var store = Arrange<SingleCollectionLoad>(disableIndex, turnOffDatabase);
        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(nameof(SingleCollectionLoad)));
        Assert.NotNull(indexStats);
        Assert.NotNull(indexStats.ReferencedCollections);
        Assert.Equal(1, indexStats.ReferencedCollections.Count);
        Assert.Equal("Item2s", indexStats.ReferencedCollections.First());
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(TestMatrix))]    
    public void MultipleCollectionLoadTest(bool disableIndex, bool turnOffDatabase)
    {
        using var store = Arrange<MultipleCollectionLoad>(disableIndex, turnOffDatabase);
        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(nameof(MultipleCollectionLoad)));
        Assert.NotNull(indexStats);
        Assert.NotNull(indexStats.ReferencedCollections);
        Assert.Equal(2, indexStats.ReferencedCollections.Count);
        Assert.Contains("Item2s", indexStats.ReferencedCollections);
        Assert.Contains("Item3s", indexStats.ReferencedCollections);
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(TestMatrix))]    
    public void MultiMapLoadTest(bool disableIndex, bool turnOffDatabase)
    {
        using var store = Arrange<MultiMapLoad>(disableIndex, turnOffDatabase);
        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(nameof(MultiMapLoad)));
        Assert.NotNull(indexStats);
        Assert.NotNull(indexStats.ReferencedCollections);
        Assert.Equal(2, indexStats.ReferencedCollections.Count);
        Assert.Contains("Item2s", indexStats.ReferencedCollections);
        Assert.Contains("Item3s", indexStats.ReferencedCollections);
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(TestMatrix))]    
    public void MultiMapLoadWithRepetitionTest(bool disableIndex, bool turnOffDatabase)
    {
        using var store = Arrange<MultiMapLoadWithRepetition>(disableIndex, turnOffDatabase);
        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(nameof(MultiMapLoadWithRepetition)));
        Assert.NotNull(indexStats);
        Assert.NotNull(indexStats.ReferencedCollections);
        Assert.Equal(1, indexStats.ReferencedCollections.Count);
        Assert.Contains("Item3s", indexStats.ReferencedCollections);
    }

    private IDocumentStore Arrange<TIndex>(bool disableIndex, bool turnOffDatabase) where TIndex : AbstractIndexCreationTask, new()
    {
        var documentsToInsert = new List<(object, string)>()
        {
            (new Item1("item2/1", "item3/1", "Item1/1"), "item1/1"), (new Item2("item2/1", "item3/1"), "item2/1"), (new Item3("item2/1", "item3/1"), "item3/1"),
        };
        
        var path = NewDataPath();
        var store = turnOffDatabase == false 
                ? GetDocumentStore()
                : GetDocumentStore(new Options()
                {
                    RunInMemory = false,
                    Path = path,
                });
        
        var index = new TIndex();
        index.Execute(store);

        using var session = store.OpenSession();
        foreach (var (doc, id) in documentsToInsert)
            session.Store(doc, id);

        session.SaveChanges();
        Indexes.WaitForIndexing(store);
        if (disableIndex)
            store.Maintenance.Send(new DisableIndexOperation(index.IndexName));

        if (turnOffDatabase)
        {
            var old = Databases.GetDocumentDatabaseInstanceFor(store).Result;
            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
        }
        
        
        return store;
    }

    private record Item1(string RefId2, string RefId3, string Prop, string Id = null);

    private record Item2(string RefId, string Prop, string Id = null);

    private record Item3(string RefId, string Prop, string Id = null);

    private class SingleCollectionLoad : AbstractIndexCreationTask<Item1>
    {
        public SingleCollectionLoad()
        {
            Map = item1s => from doc in item1s
                let loadDocument = LoadDocument<Item2>(doc.RefId2)
                select new { Current = doc.Prop, Loaded2 = loadDocument.Prop };
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class MultipleCollectionLoad : AbstractIndexCreationTask<Item1>
    {
        public MultipleCollectionLoad()
        {
            Map = item1s => from doc in item1s
                let loadDocument = LoadDocument<Item2>(doc.RefId2)
                let loadDocument2 = LoadDocument<Item3>(doc.RefId3)
                select new { Current = doc.Prop, Loaded2 = loadDocument.Prop, Loaded3 = loadDocument2.Prop };
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class MultiMapLoad : AbstractMultiMapIndexCreationTask
    {
        public MultiMapLoad()
        {
            AddMap<Item1>(item1s => from doc in item1s
                let loadDocument3 = LoadDocument<Item2>(doc.RefId2)
                select new { Current = doc.Prop, Loaded = loadDocument3.Prop });

            AddMap<Item2>(item2s => from doc in item2s
                let loadDocument = LoadDocument<Item3>(doc.RefId)
                select new { Current = doc.Prop, Loaded = loadDocument.Prop });

            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class MultiMapLoadWithRepetition : AbstractMultiMapIndexCreationTask
    {
        public MultiMapLoadWithRepetition()
        {
            AddMap<Item1>(item1s => from doc in item1s
                let loadDocument3 = LoadDocument<Item3>(doc.RefId2)
                select new { Current = doc.Prop, Loaded = loadDocument3.Prop });

            AddMap<Item2>(item1s => from doc in item1s
                let loadDocument = LoadDocument<Item3>(doc.RefId)
                select new { Current = doc.Prop, Loaded = loadDocument.Prop });

            StoreAllFields(FieldStorage.Yes);
        }
    }

    public RavenDB_22196(ITestOutputHelper output) : base(output)
    {
    }

    private static IEnumerable<object[]> TestMatrix()
    {
        foreach (var disableIndex in new[] { true, false })
        {
            foreach (var turnOffDatabase in new[] { true, false })
            {
                yield return new object[] { disableIndex, turnOffDatabase };
            }
        }
    }
}
