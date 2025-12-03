using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Raven.Server.Documents.Indexes.VectorSearch;
using Voron.Data.Graphs;

namespace Voron.Benchmark.Corax;

public class VectorSearchBenchmark
{
    [Params(360, 30000)]
    public int Size;

    private StorageEnvironment _env;
    private const string Path = "D:\\temp\\corax";
    private IndexFieldsMapping _mapping;
    private List<string> _ids;

    
    
    [GlobalSetup]
    public void GlobalSetup()
    {
        _ids = new();
        _env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(Path));
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id")
            .AddBinding(1, "name")
            .AddBinding(2, "vector", vectorOptions: new VectorOptions
            {
                NumberOfCandidates = 16, NumberOfEdges = 12, VectorEmbeddingType = VectorEmbeddingType.Single
            })
            .Build();

        using (var indexWriter = new IndexWriter(_env, _mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < Size; ++i)
            {
                var id = $"doc/{i}";
                _ids.Add(id);
                using var builder = indexWriter.Index(id);
                builder.Write(0, Encoding.UTF8.GetBytes(id));
                builder.Write(1, Encoding.UTF8.GetBytes($"name{i}"));
                var x = MathF.Cos(i * 2 * MathF.PI / 360);
                var y = MathF.Sin(i * 2 * MathF.PI / 360);
                float[] vec = [x, y];
                builder.WriteVector(2, "vector", MemoryMarshal.Cast<float, byte>(vec));
                builder.EndWriting();
            }

            indexWriter.Commit();
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _mapping?.Dispose();
        _env?.Dispose();
        Directory.Delete(Path, true);
    }

    [Benchmark]
    public int VectorQuery()
    {
        using var indexSearcher = new IndexSearcher(_env, _mapping);
        var x = MathF.Cos(180 * 2 * MathF.PI / 360);
        var y = MathF.Sin(180 * 2 * MathF.PI / 360);
        float[] vec = [x, y];
        var v = GenerateEmbeddings.FromArray(indexSearcher.Allocator, vec, Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single, Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single);
        var vs = indexSearcher.VectorSearch(_mapping.GetByFieldId(2).Metadata, v, 0.0f, 16, false, false);
        Span<long> ids = stackalloc long[16];
        var count = 0;
        while (vs.Fill(ids) is var read and > 0)
            count += read;
        return count;
    }
}
