using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    public sealed unsafe class PrefixTreePage
    {
        public readonly byte* Pointer;
        public readonly int PageSize;
        public readonly string Source;

        private PrefixTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (PrefixTreePageHeader*)Pointer; }
        }

        public byte* DataPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer + sizeof(PrefixTreePageHeader); }
        }

        public PrefixTreePage(byte* pointer, string source, int pageSize)
        {
            Pointer = pointer;
            Source = source;
            PageSize = pageSize;            
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PageNumber = value; }
        }

        /// <summary>
        /// This is the tree number following the growth strategy for the tree structure. This virtual chunks
        /// are used to navigate the whole-tree in a cache concious fashion and are part of a virtual numbering of the nodes
        /// used for fast retrieval of node offsets.
        /// </summary>
        /// <remarks>
        /// While we would try to ensure multiple trees to share as much as possible chunks we cannot ensure 
        /// that is going to be the case without running a defrag operation. 
        /// </remarks>
        public long Chunk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Chunk; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Chunk = value; }
        }

        public ushort NodesPerChunk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->NodesPerChunk; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->NodesPerChunk = value; }
        }

        public PtrBitVector FreeSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new PtrBitVector((ulong*)DataPointer + Header->NodesPerChunk * sizeof(PrefixTree.Node), Header->NodesPerChunk); }
        }

        public override string ToString()
        {
            return $"#{PageNumber}";
        }

        public void Initialize()
        {
            byte* freeSpace = DataPointer + Header->NodesPerChunk * sizeof(PrefixTree.Node);
            Memory.SetInline(freeSpace, 0xFF, Header->NodesPerChunk / BitVector.BitsPerByte + 1);
        }

        /// <summary>
        /// Will return the offset from the <see cref="DataPointer"/> to the Node.
        /// </summary>
        /// <remarks>This method will not do any bound check</remarks>
        /// <param name="nodeNameInTree">the relative name of the node in the tree.</param>
        /// <returns>the offset in the page memory for that node</returns>
        public static long GetNodeOffset(long nodeNameInTree)
        {            
            // TODO: Check if the formula is correct. 
            return nodeNameInTree * sizeof(PrefixTree.Node);
        }
    }
}
