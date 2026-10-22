using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Nito.Disposables;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using ClientEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Corax.Vectors;

public class VectorQueryCorrectnessTests(ITestOutputHelper output) : StorageTest(output)
{
    private const int TagFieldId = 2;

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void SearchByDocumentIdUsesEveryStoredVector()
    {
        using var _ = GetMappings(out var mapping, out var getVector);
        var ids = Index(mapping, getVector, [("a", [[1f, 0f], [-1f, 0f]]), ("a", [[1f, 0.1f]]), ("a", [[-1f, -0.1f]])]);

        using var indexSearcher = new IndexSearcher(Env, mapping);
        var found = All(indexSearcher.VectorSearch(mapping.GetByFieldId(1).Metadata, "vectors/1", 0.9f, 16, true, false));
        Assert.Equal(ids.Order(), found.Order());
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(true)]
    [InlineData(false)]
    public void StreamedResultsHoldEachEntryOnceNearestFirst(bool isExact)
    {
        using var _ = GetMappings(out var mapping, out var getVector);
        var ids = Index(mapping, getVector, [("a", [[1f, 0f], [1f, 0.05f]]), ("a", [[1f, 0.1f]])]);

        using var indexSearcher = new IndexSearcher(Env, mapping);
        Span<long> matches = stackalloc long[16];
        var read = indexSearcher.VectorSearch(mapping.GetByFieldId(1).Metadata, getVector([1f, 0f]), 0.9f, 16, isExact, true).Fill(matches);
        Assert.Equal(ids, matches[..read].ToArray());
    }

    private static List<long> All(IQueryMatch match)
    {
        var all = new List<long>();
        var buffer = new long[256];
        for (int read; (read = match.Fill(buffer)) > 0;)
            all.AddRange(buffer[..read]);
        return all;
    }

    private long[] Index(IndexFieldsMapping mapping, Func<float[], VectorValue> getVector, (string Tag, float[][] Vectors)[] docs)
    {
        var ids = new long[docs.Length];
        using var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (var i = 0; i < docs.Length; i++)
        {
            var id = $"vectors/{i + 1}";
            using var entry = writer.Index(id);
            entry.Write(0, Encodings.Utf8.GetBytes(id));
            entry.Write(TagFieldId, Encodings.Utf8.GetBytes(docs[i].Tag));
            if (docs[i].Vectors.Length > 1)
                entry.IncrementList();
            foreach (var vector in docs[i].Vectors)
            {
                using var value = getVector(vector);
                entry.WriteVector(1, "Vector", value.GetEmbedding());
            }

            if (docs[i].Vectors.Length > 1)
                entry.DecrementList();
            entry.EndWriting();
            ids[i] = (long)entry.EntryId;
        }

        writer.Commit();
        return ids;
    }

    private static IDisposable GetMappings(out IndexFieldsMapping mapping, out Func<float[], VectorValue> getVector)
    {
        var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var mappingLocal = IndexFieldsMappingBuilder
            .CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Vector", vectorOptions: new VectorOptions { NumberOfEdges = 4, NumberOfCandidates = 16, VectorEmbeddingType = VectorEmbeddingType.Single })
            .AddBinding(TagFieldId, "Tag")
            .Build();

        mapping = mappingLocal;
        getVector = vector => GenerateEmbeddings.FromArray(bsc, vector, ClientEmbeddingType.Single, ClientEmbeddingType.Single);

        return Disposable.Create(() =>
        {
            bsc.Dispose();
            mappingLocal.Dispose();
        });
    }
}
