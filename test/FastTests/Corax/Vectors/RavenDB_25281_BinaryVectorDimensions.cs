using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace FastTests.Corax.Vectors;

// Verifies the dimension-unit contract for BINARY (int1) vector fields (RavenDB-25281 / #4887).
//
// The query-time guard (QueryPlanBuilder.Vector.AssertDimensions) compares the field's persisted
// `numberOfDimensions` against the query vector's byte length (VectorValue.Length). For Single/Int8,
// IndexFieldsPersistence persists the BYTE length (dims*4, dims+4), so the raw comparison is correct. For
// Binary it persists the RAW dimension count, while the runtime binary vector is bit-packed to ceil(dims/8)
// bytes — so when the dimension comes from the field configuration (rather than from already-indexed data),
// a valid same-dimension binary vector trips the guard and throws "different number of dimensions".
//
// This test pins down that contract: a binary vector field configured with explicit Dimensions, queried with
// a correctly-sized binary vector, must NOT throw a dimensions mismatch.
public class RavenDB_25281_BinaryVectorDimensions(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Dimensions = 32;          // 32 bits ...
    private const int PackedBytes = Dimensions / 8; // ... = 4 bytes once bit-packed (ceil(32/8))

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public async Task BinaryVectorSearch_WithMatchingDimensions_DoesNotThrowDimensionMismatch(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            // A document without the vector field, so the field's dimension is taken from the configured
            // Dimensions (the seed path) rather than from indexed vector data.
            await session.StoreAsync(new Doc { Text = "no vector here" });
            await session.SaveChangesAsync();
        }

        await new BinaryVectorIndex().ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            // A correctly-sized binary query vector: PackedBytes bytes == Dimensions bits.
            var query = session.Query<Doc, BinaryVectorIndex>()
                .VectorSearch(x => x.WithField(f => f.Vector), f => f.ByEmbedding(new byte[PackedBytes]));

            // Must not throw "Vector field ... has N dimensions, but the vector passed ... has M dimensions".
            var results = await query.ToListAsync();
            Assert.Empty(results); // no indexed vectors, but the dimension guard must have accepted the query
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public byte[] Embedding { get; set; }
        public string Text { get; set; }
        public object Vector { get; set; }
    }

    private class BinaryVectorIndex : AbstractIndexCreationTask<Doc>
    {
        public BinaryVectorIndex()
        {
            Map = docs => from doc in docs
                select new { Vector = CreateVector(doc.Embedding) };

            VectorIndexes.Add(x => x.Vector, new VectorOptions
            {
                SourceEmbeddingType = VectorEmbeddingType.Binary,
                DestinationEmbeddingType = VectorEmbeddingType.Binary,
                Dimensions = Dimensions
            });
        }
    }
}
