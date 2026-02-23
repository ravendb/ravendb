using System;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe struct Node
    {
        internal const long VectorIdMask = ~0xFFF;
        public long PostingListId;
        public long VectorId;
        public long NodeId;
        public NativeList<NativeList<long>> EdgesPerLevel;
        public NativeList<NativeList<int>> EdgesIndexesPerLevel;
        private UnmanagedSpan _vectorSpan;
        public int Visited;
        public float? QueryDistance;

        public bool VectorLoaded => _vectorSpan.Length > 0;

        public long GetVectorContainerId()
        {
            if ((VectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
                return VectorId;

            return VectorId & VectorIdMask;
        }

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.GetReadOnly(llt, new ContainerEntryId(id));
            return Decode(llt, span);
        }

        public static NodeReader Decode(LowLevelTransaction llt, Span<byte> span)
        {
            var postingListId = VariableSizeEncoding.Read<long>(span, out var pos);
            var offset = pos;
            var vectorId = VariableSizeEncoding.Read<long>(span, out pos, offset);
            offset += pos;
            var countOfLevels = VariableSizeEncoding.Read<int>(span, out pos, offset);
            offset += pos;

            return new NodeReader(llt.Allocator, span[offset..]) { PostingListId = postingListId, VectorId = vectorId, CountOfLevels = countOfLevels };
        }

        public Span<byte> Encode(ref ContextBoundNativeList<byte> buffer)
        {
            int countOfLevels = EdgesPerLevel.Count;

            // posting list id, vector id, count of levels
            var maxSize = 3 * VariableSizeEncoding.MaximumSizeOf<long>();
            for (int i = 0; i < countOfLevels; i++)
            {
                maxSize += EdgesPerLevel[i].Count * VariableSizeEncoding.MaximumSizeOf<long>();
            }

            buffer.EnsureCapacityFor(maxSize);

            var bufferSpan = buffer.ToFullCapacitySpan();

            var pos = VariableSizeEncoding.Write(bufferSpan, PostingListId);
            pos += VariableSizeEncoding.Write(bufferSpan, VectorId, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, countOfLevels, pos);

            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = EdgesPerLevel[i].ToSpan();
                int len = Sorting.SortAndRemoveDuplicates(span);
                span = span[..len];
                long prev = 0;
                pos += VariableSizeEncoding.Write(bufferSpan, span.Length, pos);
                for (int j = 0; j < span.Length; j++)
                {
                    var delta = span[j] - prev;
                    prev = span[j];
                    pos += VariableSizeEncoding.Write(bufferSpan, delta, pos);
                }
            }

            return bufferSpan[..pos];
        }

        public UnmanagedSpan GetVectorUnmanagedSpan(SearchState state)
        {
            if (_vectorSpan.Length > 0)
                return _vectorSpan;

            _vectorSpan = NodeReader.ReadVector(VectorId, in state);
            return _vectorSpan;
        }

        public Span<byte> GetVector(SearchState state)
        {
            return GetVectorUnmanagedSpan(state).ToSpan();
        }

        internal void SetVector(SearchState searchState, UnmanagedSpan span)
        {
            if ((VectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
            {
                _vectorSpan = span;
                return;
            }

            var count = (byte)(VectorId >> 1);
            var offset = count * searchState.Options.VectorSizeBytes;
            _vectorSpan = new UnmanagedSpan(span.Address + offset, searchState.Options.VectorSizeBytes);
        }
    }
}
