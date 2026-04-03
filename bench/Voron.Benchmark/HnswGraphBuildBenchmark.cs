using System;
using System.Runtime.InteropServices;
using System.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Voron;
using Voron.Data.Graphs;

namespace Voron.Benchmark;

/// <summary>
/// Benchmarks HNSW graph construction to measure allocation pressure
/// from WorkItem scheduling during parallel graph building.
/// Run: dotnet run -c Release -- --filter "*Hnsw*"
/// </summary>
[MemoryDiagnoser]
[Config(typeof(Config))]
public class HnswGraphBuildBenchmark
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(Job.ShortRun
                .WithWarmupCount(2)
                .WithIterationCount(5));
            WithBuildTimeout(TimeSpan.FromMinutes(10));
        }
    }

    private const int Dimensions = 16;
    private const int VectorSizeBytes = Dimensions * sizeof(float);
    private const int NumberOfNodes = 65536;

    // HNSW parameters
    private const int NumberOfEdges = 8; // M
    private const int NumberOfCandidates = 16; // efConstruction

    private float[][] _vectors;
    private StorageEnvironment _env;
    private StorageEnvironmentOptions _options;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        _vectors = new float[NumberOfNodes][];
        for (int i = 0; i < NumberOfNodes; i++)
        {
            _vectors[i] = new float[Dimensions];
            for (int d = 0; d < Dimensions; d++)
            {
                _vectors[i][d] = rng.NextSingle() * 2f - 1f;
            }
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _options = StorageEnvironmentOptions.CreateMemoryOnlyForTests("HnswBench");
        _env = new StorageEnvironment(_options);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _env?.Dispose();
        _options?.Dispose();
    }

    [Benchmark(OperationsPerInvoke = NumberOfNodes)]
    public void BuildHnswGraph()
    {
        var hnswRandom = new Random(42);

        using var txw = _env.WriteTransaction();
        Hnsw.Create(txw.LowLevelTransaction, "vectors", VectorSizeBytes, NumberOfEdges, NumberOfCandidates, VectorEmbeddingType.Single);

        using var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "vectors", hnswRandom);
        for (int i = 0; i < NumberOfNodes; i++)
        {
            registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(_vectors[i]));
        }
        registration.Commit(CancellationToken.None);
        txw.Commit();
    }
}
