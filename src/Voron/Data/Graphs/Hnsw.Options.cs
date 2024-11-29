using System.Numerics;
using System.Runtime.InteropServices;
using Voron.Impl.Paging;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 64)]
    public struct Options
    {
        [FieldOffset(0)]
        public int VectorSizeBytes;

        [FieldOffset(4)] // M - Number of edges
        public int NumberOfEdges;

        [FieldOffset(8)] // EfConstruction - Number of candidates 
        public int NumberOfCandidates;

        [FieldOffset(12)] // this is used only in debug, not important for persistence
        public ushort Version;

        [FieldOffset(14)]
        public byte VectorBatchIndex;

        [FieldOffset(15)]
        public SimilarityMethod SimilarityMethod;

        [FieldOffset(16)]
        public long CountOfVectors;

        [FieldOffset(24)]
        public long Container;

        [FieldOffset(32)]
        public long LastUsedContainerId;

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
            _ => Paging.GetNumberOfOverflowPages(VectorSizeBytes * 5)
        };
    }
}
