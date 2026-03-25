using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Vector;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23657(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public async Task CanCreateAutoIndexFromListOfRavenVector()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(CreateDocument());

        await session.SaveChangesAsync();
        //Sbyte
        var results = await session.Query<Document>()
            .Statistics(out var statistics)
            .Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(s => s.TagsEmbeddedAsInt8, VectorEmbeddingType.Int8),
                v => v.ByEmbedding(new RavenVector<sbyte>(VectorQuantizer.ToInt8([1, 2]))))
            .ToListAsync();
        Assert.Single(results);

        await store.Maintenance.SendAsync(new DeleteIndexOperation(statistics.IndexName));

        //Floats
        results = await session.Query<Document>()
            .Statistics(out statistics)
            .Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(s => s.TagsEmbeddedAsSingle),
                v => v.ByEmbedding(new RavenVector<float>([.1f, .2f])))
            .ToListAsync();
        Assert.Single(results);

        await store.Maintenance.SendAsync(new DeleteIndexOperation(statistics.IndexName));

        //Int1
        results = await session.Query<Document>()
            .Statistics(out statistics)
            .Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(s => s.TagsEmbeddedAsBinary, VectorEmbeddingType.Binary),
                v => v.ByEmbedding(new RavenVector<byte>(VectorQuantizer.ToInt1([0.1f, 0.2f, -1f, 0, 1, 0, 0]))))
            .ToListAsync();

        WaitForUserToContinueTheTest(store);
        Assert.Single(results);
    }
    
    [RavenFact(RavenTestCategory.Vector)]
    public async Task RavenVectorUnderlyingValuesWillBeCorrectlyStored()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenAsyncSession();

        await session.StoreAsync(CreateDocument());
        await session.SaveChangesAsync();

        await new IndexType().ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);
        IndexType.Map underlyingTypes = await session.Query<IndexType.Map, IndexType>().ProjectInto<IndexType.Map>().SingleAsync();
        //RavenVector<byte>
        Assert.Equal("Byte", underlyingTypes.TypeBinary[0]);
        Assert.Equal("SByte", underlyingTypes.TypeBinary[1]);

        //RavenVector<sbyte>
        Assert.Equal("SByte", underlyingTypes.TypeInt8[0]);
        Assert.Equal("SByte", underlyingTypes.TypeInt8[1]);

        //RavenVector<float>
        Assert.Equal("Float", underlyingTypes.TypeSingle[0]);
        Assert.Equal("Float", underlyingTypes.TypeSingle[1]);
    }

    [RavenFact(RavenTestCategory.Vector)]
    public async Task CanIndexRavenVectorOfInt32AsSingle()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenAsyncSession();

        await session.StoreAsync(CreateDocument());
        await session.SaveChangesAsync();
        
        var result = await session.Query<Document>().Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(s => s.TagsEmbeddedAsInt), p => p.ByEmbedding(new RavenVector<int>([1, 2, 3])))
            .ToArrayAsync();
        Assert.Single(result);
    }

    private static Document CreateDocument() => new Document()
    {
        TagsEmbeddedAsBinary = new()
        {
            new RavenVector<byte>(VectorQuantizer.ToInt1([0.1f, 0.2f, -1f, 0, 1, 0, 0])),
            new RavenVector<byte>(VectorQuantizer.ToInt1([-0.1f, 0.2f, -1f, 0, -1, 0, 0]))
        },
        TagsEmbeddedAsInt8 = new List<RavenVector<sbyte>>([
            new RavenVector<sbyte>(VectorQuantizer.ToInt8([1, 2])),
            new RavenVector<sbyte>(VectorQuantizer.ToInt8([3, 4]))
        ]),
        TagsEmbeddedAsSingle = new() { new RavenVector<float>([.1f, .2f]), new RavenVector<float>([-.1f, -.2f]), },
        TagsEmbeddedAsInt = [new RavenVector<int>([1, 2, 3]), new RavenVector<int>([-1, -2, -3])]
    };

    private class Document
    {
        public List<RavenVector<sbyte>> TagsEmbeddedAsInt8 { get; set; }
        public List<RavenVector<float>> TagsEmbeddedAsSingle { get; set; }
        public List<RavenVector<byte>> TagsEmbeddedAsBinary { get; set; }
        public List<RavenVector<int>> TagsEmbeddedAsInt { get; set; }
    }

    private class IndexType : AbstractIndexCreationTask
    {
        public class Map
        {
            public string[] TypeBinary { get; set; }
            public string[] TypeInt8 { get; set; }
            public string[] TypeSingle { get; set; }
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition()
            {
                Maps =
                [
                    @"
from doc in docs.Documents 
select new 
{
    TypeBinary = Helper.GetUnderlyingEmbeddingType(doc.TagsEmbeddedAsBinary),
    TypeInt8 = Helper.GetUnderlyingEmbeddingType(doc.TagsEmbeddedAsInt8),
    TypeSingle = Helper.GetUnderlyingEmbeddingType(doc.TagsEmbeddedAsSingle)
}"
                ],
                AdditionalSources = new()
                {
                    {
                        "Helper", @"
using Sparrow.Json;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

public static class Helper
{
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = ""_inner"")]
    private static extern ref IEnumerable<object> GetInnerField(DynamicArray dynamicArray);

    public static List<string> GetUnderlyingEmbeddingType(object value)
    {
        var bjra = value as BlittableJsonReaderArray;
        if (value is DynamicArray da)
        {
            bjra = (BlittableJsonReaderArray)GetInnerField(da);
        }
        List<string> values = new();

        if (bjra == null)
        {
            values.Add(value.GetType().FullName);
            return values;
        }

        foreach (var currentObject in bjra)
        {
            var bjro = currentObject as BlittableJsonReaderObject ?? ((DynamicBlittableJson)value).BlittableJson;
            if (bjro != null && bjro.TryGetMember(""@vector"", out var vector)
                 && vector is BlittableJsonReaderVector bjrv)
            {
                values.Add(bjrv.Type.ToString());
            }
        }

        if (values.Count == 0)
            values.Add(""Unknown"");

        return values;
    }
}"
                    }
                },
                Fields = new Dictionary<string, IndexFieldOptions>()
                {
                    { "TypeBinary", new IndexFieldOptions() { Storage = FieldStorage.Yes } },
                    { "TypeInt8", new IndexFieldOptions() { Storage = FieldStorage.Yes } },
                    { "TypeSingle", new IndexFieldOptions() { Storage = FieldStorage.Yes } },
                }
            };
        }
    }
}
