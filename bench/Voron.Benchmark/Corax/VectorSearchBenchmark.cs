using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Utils;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data.Graphs;

namespace Voron.Benchmark.Corax;

public class VectorSearchBenchmark
{
    [Params(30_000, 100_000)]
    public int Size;

    [Params(16, 64, 128)]
    public int NumberOfCandidates;
    
    private StorageEnvironment _env;
    private const string Path = "D:\\temp\\corax";
    private IndexFieldsMapping _mapping;
    private List<string> _ids;
    private List<string> _sources;
    private IndexSearcher _indexSearcher;
    private byte[] QueryVector;
    

    [GlobalSetup]
    public void GlobalSetup()
    {
        Random random = new(412312345);
        {
            var words = new List<string>();
            var file = Directory.GetFiles(Directory.GetCurrentDirectory(), "?nglish.txt").Single();
            foreach (var line in File.ReadAllLines(file))
                    words.Add(line);
            random.Shuffle(CollectionsMarshal.AsSpan(words));
            _sources = words.Take(Size).ToList();
        }

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
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            for (int i = 0; i < Size; ++i)
            {
                var id = $"doc/{i}";
                _ids.Add(id);
                using var builder = indexWriter.Index(id);
                builder.Write(0, Encoding.UTF8.GetBytes(id));
                builder.Write(1, Encoding.UTF8.GetBytes($"name{i}"));
                using var vec = GenerateEmbeddings.FromText(bsc,
                    new Raven.Client.Documents.Indexes.Vector.VectorOptions() { DestinationEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single, SourceEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Text },
                    _sources[i]);
                builder.WriteVector(2, "vector", vec.GetEmbedding());
                builder.EndWriting();
            }

            var vecToSearch = random.Next(Size);
            using var vecToSearchVV = GenerateEmbeddings.FromText(bsc,
                new Raven.Client.Documents.Indexes.Vector.VectorOptions() { DestinationEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single, SourceEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Text },
                _sources[vecToSearch]);
            QueryVector = vecToSearchVV.GetEmbedding().ToArray();

            indexWriter.Commit();
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _ids = null;
        _sources = null;
        _mapping?.Dispose();
        _env?.Dispose();
        Directory.Delete(Path, true);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _indexSearcher = new IndexSearcher(_env, _mapping);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _indexSearcher.Dispose();
    }
    
    [Benchmark]
    public int VectorQuery()
    {
        var v = new VectorValue(null, QueryVector);
        var vs = _indexSearcher.VectorSearch(_mapping.GetByFieldId(2).Metadata, v, 0.0f, NumberOfCandidates, false, false);
        Span<long> ids = stackalloc long[16];
        var count = 0;
        while (vs.Fill(ids) is var read and > 0)
            count += read;
        return count;
    }
}
