using System;
using System.Numerics;
using System.Runtime.InteropServices;
using Voron.Data.Containers;
using Voron.Impl.Paging;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 64)]
    public struct Options
    {
        [FieldOffset(0)]
        public int VectorSizeBytes;

        // The number of edges in a list of an upper level (M in the HNSW paper). A layer 0 list holds more, see MaxEdgesAtLevel.
        [FieldOffset(4)]
        public int NumberOfEdges;

        // The number of candidates that the search for the neighbors of a new node keeps (efConstruction in the HNSW paper).
        [FieldOffset(8)]
        public int NumberOfCandidates;

        [FieldOffset(12)]
        public ushort Version;

        [FieldOffset(14)]
        public byte VectorBatchIndex;

        [FieldOffset(15)]
        public SimilarityMethod SimilarityMethod;

        [FieldOffset(16)]
        public long CountOfVectors;

        [FieldOffset(24)]
        public ContainerId Container;

        [FieldOffset(32)]
        public ContainerId LastUsedContainerId;

        public int MaxEdgesAtLevel(int level)
        {
            // Upper levels only move a query near its target.
            if (level != 0)
                return NumberOfEdges;

            // A query finds its results in layer 0, so it gets more paths there (Mmax0 = 2 * M in the HNSW paper).
            int twoTimesNumberOfEdges = NumberOfEdges * 2;

            // Additional edges use candidates that the search keeps anyway; with fewer candidates, they would make the search wider.
            if (NumberOfCandidates < twoTimesNumberOfEdges)
                return twoTimesNumberOfEdges;

            // Each additional edge is one more read for a query, so they are few.
            int additionalEdges = NumberOfEdges / 8;
            return twoTimesNumberOfEdges + additionalEdges;
        }

        // The search for the neighbors of a new node keeps at least as many candidates as the list holds. Thus the list of a new
        // node can be full.
        public int CandidatesAtLevel(int level)
        {
            int maxEdges = MaxEdgesAtLevel(level);
            return Math.Max(NumberOfCandidates, maxEdges);
        }

        public int PrunedFillLimitAtLevel(int level)
        {
            // The candidates that FilterEdgesHeuristicWorker rejected fill the list up to this limit.
            int maxEdges = MaxEdgesAtLevel(level);

            // Upper level lists get few back-links.
            if (level != 0)
                return maxEdges;

            // With this many candidates, every slot gets a near node.
            if (NumberOfCandidates >= NumberOfEdges * 2)
                return maxEdges;

            // A free slot takes a back-link without a new selection of the list, and a query reads fewer edges.
            int freeSlots = NumberOfEdges / 3;
            return maxEdges - freeSlots;
        }

        public int MaxLevel => BitOperations.Log2((ulong)CountOfVectors);

        public int CurrentMaxLevel(int reduceBy) => BitOperations.Log2((ulong)(CountOfVectors - reduceBy));

        // Remember that we have to fit a page header, and we want to
        // minimize wasted space, so we will allocate the vectors in batches.
        public int VectorBatchInPages => VectorSizeBytes switch
        {
            < 768 => 1, // small enough it doesn't matter, use a 

            768 => 2, // 2 pages, 21 vectors,   192 bytes wasted,  10 bytes / vector wasted
            1024 => 2, // 2 pages, 15 vectors,   960 bytes wasted,  64 bytes / vector wasted
            1536 => 4, // 4 pages, 21 vectors,   448 bytes wasted,  22 bytes / vector wasted

            < 2048 => 4,
            2048 => 4, // 4 pages, 15 vectors, 1,984 bytes wasted, 138 bytes / vector wasted
            3072 => 2, // 2 pages,  5 vectors,   960 bytes wasted, 192 bytes / vector wasted
            < 4096 => 4,
            4096 => 4, // 4 pages,  7 vectors, 4,032 bytes wasted, 576 bytes / vector wasted
            6144 => 4, // 4 pages,  5 vectors, 1,984 bytes wasted, 375 bytes / vector wasted
            12288 => 8, // 8 pages, 5 vectors, 4,032 bytes wasted, 807 bytes / vector wasted

            // in all sizes, we get a pretty good rate if we go with 5 vectors in a batch
            _ => VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(VectorSizeBytes * 5)
        };
    }
}
