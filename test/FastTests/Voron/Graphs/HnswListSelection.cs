using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class HnswListSelection(ITestOutputHelper output) : StorageTest(output)
{
    // The same vectors, levels and batches are placed selecting overfull lists after each batch and on every overflow.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ListsAreSelectedOnceAfterTheBatch()
    {
        var afterTheBatch = Build("after", selectsOnOverflow: false, batchSize: 300, maxConcurrentBatches: null);
        var onOverflow = Build("overflow", selectsOnOverflow: true, batchSize: 300, maxConcurrentBatches: null);
        Assert.True(afterTheBatch * 10 < onOverflow, $"lists were selected {afterTheBatch} times during placement after the batch and {onOverflow} times on overflow");
    }

    // Nodes placed at the same time add each other as edges before their candidates, so concurrent placements may start a list over its cap.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ListsStayWithinTheirCapWithConcurrentPlacements()
    {
        Build("concurrent", selectsOnOverflow: false, batchSize: 1200, maxConcurrentBatches: 100);
    }

    private int Build(string name, bool selectsOnOverflow, int batchSize, int? maxConcurrentBatches)
    {
        var random = new Random(7);
        var vectors = Enumerable.Range(0, 1200).Select(_ => Enumerable.Range(0, 16).Select(_ => random.NextSingle() * 2 - 1).ToArray()).ToArray();
        using var _ = Slice.From(Allocator, name, out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, 16 * sizeof(float), 8, 32, VectorEmbeddingType.Single);
            tx.Commit();
        }

        var reselections = 0;
        for (int start = 0; start < vectors.Length; start += batchSize)
        {
            using var tx = Env.WriteTransaction();
            using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
            {
                var testing = registration.ForTestingPurposesOnly();
                testing.SelectsListsOnOverflow = selectsOnOverflow;
                testing.BeforeEdgesReplaced = (_, _, _) => reselections++;
                if (maxConcurrentBatches is not null)
                    registration.MaxConcurrentBatches = maxConcurrentBatches.Value;
                for (int i = start; i < start + batchSize; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName))
        {
            var count = state.Options.CountOfVectors;
            var inDegree = new int[count + 1];
            for (long nodeId = 1; nodeId <= count; nodeId++)
            {
                ref var node = ref state.GetDecodedNode(state.GetNodeIndexById(nodeId));
                for (int level = 0; level < node.EdgesPerLevel.Count; level++)
                {
                    int edges = node.EdgesPerLevel[level].Count;
                    Assert.True(edges <= state.Options.MaxEdgesAtLevel(level), $"the list of {nodeId} at level {level} holds {edges} edges");
                }

                foreach (var id in node.EdgesPerLevel[0].ToSpan())
                    inDegree[id]++;
            }

            for (long nodeId = 1; nodeId <= count; nodeId++)
                Assert.Equal(inDegree[nodeId], state.GetNodeByIndex(state.GetNodeIndexById(nodeId)).InDegree);
        }

        return reselections;
    }
}
