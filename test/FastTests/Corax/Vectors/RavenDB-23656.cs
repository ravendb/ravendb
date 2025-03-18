using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class RavenDB_23656(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void DifferentDimensionsWillNotCrashTheServer()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();
        session.Store(new Vector
        {
            Array =
            [
                [
                    106,
                    127,
                    -102,
                    -103,
                    25,
                    63
                ],
                [
                    111,
                    127,
                    -51,
                    -52,
                    76,
                    63
                ]
            ]
        });
        session.SaveChanges();

        var result = session.Query<Vector>()
            .Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(x => x.Array, VectorEmbeddingType.Int8),
                v => v.ByEmbedding(new sbyte[] { 106, 127, -102, -103, 25, 63 }))
            .ToArray();

        Assert.Equal(1, result.Length);

        //query with different dimension size
        var ex = Assert.Throws<RavenException>(() => session.Query<Vector>()
            .Customize(c => c.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(x => x.Array, VectorEmbeddingType.Int8),
                v => v.ByEmbedding(new sbyte[] { 106, 127, -102, -103, 25 }))
            .ToArray());

        Assert.Contains("Vector field `vector.search(embedding.i8(Array))` has 2 dimensions, but the vector passed to vector.search() has 1 dimensions", ex.Message);
    }

    private class Vector
    {
        public sbyte[][] Array { get; set; }
    }
}
