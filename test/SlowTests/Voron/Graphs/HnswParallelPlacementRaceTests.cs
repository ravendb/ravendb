using System;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace SlowTests.Voron.Graphs;

public unsafe class HnswParallelPlacementRaceTests(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int VectorDimensions = 16;
    private const int VectorSizeInBytes = VectorDimensions * sizeof(float);

    // RavenDB-26809: reproduces the actual use-after-free. A placement worker is parked while it holds a
    // reference into a node's edges, then the LLT thread moves both that edge buffer and the node array
    // before the worker resumes. With the fix (per-task edge snapshot + retained node buffer) the build
    // completes and the graph stays queryable; reverting either guard crashes here under the parked read.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ParallelPlacementSurvivesEdgeAndNodeReallocUnderParkedWorker()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        // Enough vectors that several placement tasks run concurrently, so the parked victim is never the
        // only outstanding work and the runner keeps looping.
        const int vectorCount = 4000;
        var random = new Random(42);

        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, VectorSizeInBytes, numberOfEdges: 12,
                numberOfCandidates: 16, VectorEmbeddingType.Single);

            using var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random);
            registration.ForTestingPurposesOnly().SimulateConcurrentRealloc = true;

            for (int i = 1; i <= vectorCount; i++)
                registration.Register(i, MemoryMarshal.Cast<float, byte>(RandomVector(random)));

            registration.Commit(CancellationToken.None);
            tx.Commit();

            var testing = registration.ForTestingPurposesOnly();
            Assert.True(testing.VictimSelected,
                "no worker parked on a non-empty edge buffer — the race window was never exercised; adjust count/seed");
            Assert.True(testing.InnerEdgeBufferMovedWhileWorkerParked,
                "the edge buffer was not moved while the worker was parked — edge use-after-free window not exercised");
            Assert.True(testing.NodesArrayMovedWhileWorkerParked,
                "the node array was not moved while the worker was parked — node use-after-free window not exercised");
            Assert.True(testing.StaleNodeReferenceStillValid,
                "a node reference captured before the grow read freed/poisoned memory — node array was not retained");
        }

        float[] query = RandomVector(new Random(123));
        var queryBytes = new byte[Hnsw.TensorSizeBytes<float>(query.Length)];
        Hnsw.WriteNormalizedTensor(query, queryBytes);

        using (var tx = Env.ReadTransaction())
        {
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName);
            using var retriever = Hnsw.ApproximateNearest(state, numberOfCandidates: 32, queryBytes, minimumSimilarity: 0f);

            var scores = new float[32];
            var docs = new long[32];
            int total = 0, read;
            do
            {
                read = retriever.Fill(docs, scores, filter: null);
                total += read;
            } while (read != 0);

            Assert.True(total > 0, "post-build query returned no results — the graph was corrupted by the forced realloc");
        }
    }

    private static float[] RandomVector(Random random)
    {
        var v = new float[VectorDimensions];
        float sumSq = 0;
        for (int i = 0; i < VectorDimensions; i++)
        {
            v[i] = (float)(random.NextDouble() * 2 - 1);
            sumSq += v[i] * v[i];
        }
        var norm = MathF.Sqrt(sumSq);
        if (norm > 0)
        {
            for (int i = 0; i < VectorDimensions; i++)
                v[i] /= norm;
        }
        return v;
    }
}
