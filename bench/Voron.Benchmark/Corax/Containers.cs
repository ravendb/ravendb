using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Elastic.Clients.Elasticsearch.Xpack;
using Voron.Data.Containers;
using Microsoft.VisualBasic.FileIO;
namespace Voron.Benchmark.Corax;

public class Containers
{
    [Params(16, 128, 1024, 1024 * 100)]
    public int Size;

    private StorageEnvironment _env;
    private const string Path = "D:\\temp\\corax";
    private IndexFieldsMapping _mapping;
    private List<string> _ids;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _ids = new();
        _env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path));
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id")
            .AddBinding(1, "name")
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
    
    [Benchmark(Baseline = true)]
    public bool Delete()
    {
        using var wTx = _env.WriteTransaction();
        using var indexWriter = new IndexWriter(wTx, _mapping, SupportedFeatures.All);
        var result = true;
        foreach (var toDelete in _ids)
            result &= indexWriter.TryDeleteEntry(toDelete);
    
        indexWriter.Commit();
        
        // wTx.Commit(); We're purposely not committing the transaction to
        // persist the data on disk for the next iteration.
        
        return result;
    }
}
