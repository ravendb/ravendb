using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries.Vector;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23658(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void CanSearchForVectorFromSingle()
    {
        using var store = GetDocumentStore();
        new Index2().Execute(store);
        using var session = store.OpenSession();
        session.Store(CreateItem1());
        session.Store(CreateItem2());
        session.SaveChanges();

        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);

        var index2Results = session.Query<Index2.IndexEntry, Index2>()
            .VectorSearch(
                field => field
                    .WithField(x => x.VectorFromSingle),
                searchTerm => searchTerm
                    .ByEmbedding(new RavenVector<float>([6.599999904632568f, 7.699999809265137f])), minimumSimilarity: 0.75f)
            .ProjectInto<Item>()
            .ToList();

        Assert.Equal(1, index2Results.Count);
        Assert.Equal("Item1", index2Results[0].Name);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void CanSearchForVectorFromSBytes()
    {
        using var store = GetDocumentStore();
        new Index3().Execute(store);
        using var session = store.OpenSession();
        session.Store(CreateItem1());
        session.Store(CreateItem2());
        session.SaveChanges();

        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
        
        var index3Results = session.Query<Index3.IndexEntry, Index3>()
            .VectorSearch(
                field => field
                    .WithField(x => x.VectorFromInt8Array),
                searchTerm => searchTerm
                    .ByEmbedding(VectorQuantizer.ToInt8([0.1f, 0.2f])))
            .ProjectInto<Item>()
            .ToList();
        
        Assert.NotEmpty(index3Results);
    }

    private static Item CreateItem1() => new()
    {
        Name = "Item1",
        TagsEmbeddedAsSingle =
            new RavenVector<float>([6.599999904632568f, 7.699999809265137f]),
        TagsEmbeddedAsInt8 = new List<RavenVector<sbyte>>([
            new RavenVector<sbyte>(VectorQuantizer.ToInt8([1, 2])),
            new RavenVector<sbyte>(VectorQuantizer.ToInt8([3, 4]))
        ]),
        TagsEmbeddedAsInt8Regular = new sbyte[][] { VectorQuantizer.ToInt8([0.1f, 0.2f]), VectorQuantizer.ToInt8([0.3f, 0.4f]) }
    };

    private static Item CreateItem2() => new()
    {
        Name = "Item2",
        TagsEmbeddedAsSingle = new RavenVector<float>([8.800000190734863f, -9.899999618530273f]),
        TagsEmbeddedAsInt8 = new List<RavenVector<sbyte>>([
            new RavenVector<sbyte>(VectorQuantizer.ToInt8([5, 6])),
            new RavenVector<sbyte>(VectorQuantizer.ToInt8([7, 8]))
        ]),
        TagsEmbeddedAsInt8Regular = [VectorQuantizer.ToInt8([0.5f, 0.6f]), VectorQuantizer.ToInt8([0.7f, 0.8f])]
    };

    private class Index2 : AbstractIndexCreationTask<Item, Index2.IndexEntry>
    {
        public class IndexEntry()
        {
            public object VectorFromSingle { get; set; }
        }

        public Index2()
        {
            Map = items => from item in items
                select new IndexEntry { VectorFromSingle = CreateVector(item.TagsEmbeddedAsSingle) };

            VectorIndexes.Add(x => x.VectorFromSingle,
                new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single });

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class Index3 : AbstractIndexCreationTask<Item, Index3.IndexEntry>
    {
        public class IndexEntry()
        {
            public object VectorFromInt8Array { get; set; }
        }

        public Index3()
        {
            Map = items => from item in items
                select new IndexEntry { VectorFromInt8Array = CreateVector(item.TagsEmbeddedAsInt8Regular) };

            VectorIndexes.Add(x => x.VectorFromInt8Array,
                new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Int8, DestinationEmbeddingType = VectorEmbeddingType.Int8 });

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class Item
    {
        public string Name { get; set; }
        public RavenVector<float> TagsEmbeddedAsSingle { get; set; }

        public List<RavenVector<sbyte>> TagsEmbeddedAsInt8 { get; set; }
        public sbyte[][] TagsEmbeddedAsInt8Regular { get; set; }
    }
}
