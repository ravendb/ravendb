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

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(true)]
    [InlineData(false)]
    public void ExactFilteredSearchReturnsTheNearestMatches(bool streamed)
    {
        using var _ = GetMappings(out var mapping, out var getVector);
        var ids = Index(mapping, getVector, Enumerable.Range(0, 40).Select(i => (i % 2 == 0 ? "a" : "b", new[] { new[] { MathF.Cos(i * 0.03f), MathF.Sin(i * 0.03f) } })).ToArray());

        using var indexSearcher = new IndexSearcher(Env, mapping);
        var found = All(indexSearcher.VectorSearch(mapping.GetByFieldId(1).Metadata, getVector([1f, 0f]), 0f, 4, true, streamed, indexSearcher.TermQuery("Tag", "a")));
        long[] expected = [ids[0], ids[2], ids[4], ids[6]];
        Assert.Equal(streamed ? expected : expected.Order(), found);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void FilteredApproximateSearchIsDeterministic()
    {
        var random = new Random(3);
        using var _ = GetMappings(out var mapping, out var getVector);
        Index(mapping, getVector, Enumerable.Range(0, 1500).Select(_ => (random.Next(3) == 0 ? "a" : "b", new[] { Gaussian(random) })).ToArray());
        var query = Gaussian(random);

        using var indexSearcher = new IndexSearcher(Env, mapping);
        var runs = Enumerable.Range(0, 4).Select(_ => Run(indexSearcher, mapping, getVector(query), indexSearcher.TermQuery("Tag", "a"))).ToList();
        Assert.NotEmpty(runs[0].Entries);
        Assert.All(runs, run =>
        {
            Assert.Equal(runs[0].Entries, run.Entries);
            Assert.Equal(runs[0].Processed, run.Processed);
        });
    }

    private static (List<long> Entries, string Processed) Run(IndexSearcher indexSearcher, IndexFieldsMapping mapping, VectorValue query, IQueryMatch filter)
    {
        var match = indexSearcher.VectorSearch(mapping.GetByFieldId(1).Metadata, query, 0f, 10, false, true, filter, 0);
        var entries = All(ref match);
        return (entries, match.Inspect().Children.Single(child => child.Operation == "VectorSearchMatch").Parameters["Number of candidates scanned"]);
    }

    private static List<long> All(IQueryMatch match) => All(ref match);

    // The match is a struct: it must be drained in place for Inspect to see the retriever.
    private static List<long> All<T>(ref T match) where T : IQueryMatch
    {
        var all = new List<long>();
        var buffer = new long[256];
        for (int read; (read = match.Fill(buffer)) > 0;)
            all.AddRange(buffer[..read]);
        return all;
    }

    private static float[] Gaussian(Random random) =>
        Enumerable.Range(0, 16).Select(_ => (float)(Math.Sqrt(-2 * Math.Log(1 - random.NextDouble())) * Math.Cos(2 * Math.PI * random.NextDouble()))).ToArray();

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
