using System;
using System.Diagnostics;
using System.IO;
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

            // CountOfLevels sized the allocation above. The record must deliver exactly that
            // many level lists. The adds below are unchecked and would run past the capacity.
            var levels = 0;
            while (NextReadEdges(out var list))
            {
                if (levels == CountOfLevels)
                    throw new InvalidDataException($"Corrupt HNSW node record: it holds more level lists than the {CountOfLevels} it declares");

                node.EdgesPerLevel.AddUnsafe(list);
                levels++;
            }

            if (levels != CountOfLevels)
                throw new InvalidDataException($"Corrupt HNSW node record: it declares {CountOfLevels} level lists but holds only {levels}");
        }

        private bool NextReadEdges(out NativeList<long> list)
        {
            if (_offset >= _buffer.Length)
            {
                list = default;
                return false;
            }

            var count = ReadBoundedVarInt(_buffer, ref _offset);

            // Every edge costs at least one byte. A valid edge count cannot exceed the bytes
            // remaining. The count is persisted bytes sizing a native allocation.
            if (count < 0 || count > _buffer.Length - _offset)
                throw new InvalidDataException($"Corrupt HNSW node record: edge count {count} does not fit the {_buffer.Length - _offset} bytes remaining in the record");

            list = new NativeList<long>();
            list.EnsureCapacityFor(allocator, (int)count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                prev += ReadBoundedVarInt(_buffer, ref _offset);
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }

            return true;
        }

        /// Reads a LEB128 varint without stepping past the buffer. The record is persisted bytes.
        /// A truncated varint must fail as a corrupt-record error, not read adjacent memory.
        internal static long ReadBoundedVarInt(ReadOnlySpan<byte> buffer, ref int offset)
        {
            ulong result = 0;
            for (int shift = 0; shift < 64; shift += 7)
            {
                if (offset >= buffer.Length)
                    throw new InvalidDataException("Corrupt HNSW node record: truncated varint");
                byte b = buffer[offset++];
                result |= (ulong)(b & 0x7F) << shift;
                if (b < 0x80)
                    return (long)result;
            }
            throw new InvalidDataException("Corrupt HNSW node record: varint exceeds 64 bits");
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
