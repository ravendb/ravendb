using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe ref struct NodeReader(ByteStringContext allocator, Span<byte> buffer)
    {
        public long PostingListId;
        public long VectorId;
        public int CountOfLevels;

        private int _offset;
        private readonly Span<byte> _buffer = buffer;

        public void LoadInto(ref Node node)
        {
            node.VectorId = VectorId;
            node.PostingListId = PostingListId;
            node.EdgesPerLevel.EnsureCapacityFor(allocator, CountOfLevels);
            while (NextReadEdges(out var list))
            {
                node.EdgesPerLevel.AddUnsafe(list);
            }
        }

        private bool NextReadEdges(out NativeList<long> list)
        {
            if (_offset >= _buffer.Length)
            {
                list = default;
                return false;
            }

            var count = VariableSizeEncoding.Read<int>(_buffer, out int offset, _offset);
            _offset += offset;
            list = new NativeList<long>();
            list.EnsureCapacityFor(allocator, count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                var item = VariableSizeEncoding.Read<long>(_buffer, out offset, _offset);
                _offset += offset;
                prev += item;
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }

            return true;
        }

        public UnmanagedSpan ReadVector(in SearchState state) => ReadVector(VectorId, in state);

        public static UnmanagedSpan ReadVector(long vectorId, in SearchState state)
        {
            if ((vectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
            {
                Container.Get(state.Llt, new ContainerEntryId(vectorId), out var item);
                var vectorSpan = new UnmanagedSpan(item.Address, item.Length);
                Debug.Assert(state.Options.VectorSizeBytes == vectorSpan.Length, "state.Options.VectorSizeBytes == vectorSpan.Length");
                return vectorSpan;
            }

            var count = (byte)(vectorId >> 1);
            var containerId = vectorId & Node.VectorIdMask;
            Container.Get(state.Llt, new ContainerEntryId(containerId), out var container);
            var offset = count * state.Options.VectorSizeBytes;
            Debug.Assert(offset >= 0 && offset + state.Options.VectorSizeBytes <= container.Length, "offset >= 0 && offset + state.Options.VectorSizeBytes <= container.Length");
            return new UnmanagedSpan(container.Address + offset, state.Options.VectorSizeBytes);
        }
    }
}
