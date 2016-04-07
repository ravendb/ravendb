using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl.Paging;

namespace Voron.Data.Compact
{
    public sealed unsafe class PrefixTreePage
    {
        public readonly byte* Pointer;
        public readonly IVirtualPager Pager;

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

        public PrefixTreePage(byte* pointer, IVirtualPager pager)
        {
            Pointer = pointer;
            Pager = pager;
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
            get
            {
                byte* freeSpace = DataPointer + Header->NodesPerChunk * sizeof(PrefixTree.Node);
                return new PtrBitVector( freeSpace, Header->NodesPerChunk );
            }
        }

        public override string ToString()
        {
            return $"#{PageNumber}";
        }

        public void Initialize()
        {
            // We need to zero the values for the nodes in order to ensure that we will be getting proper data...
            // We can relax this, but then we will have to remove some runtime checks (assertions). 
            // Profile first, remove if necessary.
            Memory.SetInline(DataPointer, 0x00, Header->NodesPerChunk * sizeof(PrefixTree.Node));

            byte* freeSpace = DataPointer + Header->NodesPerChunk * sizeof(PrefixTree.Node);
            Memory.SetInline(freeSpace, 0xFF, Header->NodesPerChunk / BitVector.BitsPerByte + 1);
        }

        public PrefixTree.Node* GetNodePointer(long relativeNodeName)
        {
            if (relativeNodeName >= Header->NodesPerChunk)
                throw new InvalidOperationException("This shouldnt happen.");

            return (PrefixTree.Node*)(this.DataPointer + (relativeNodeName * sizeof(PrefixTree.Node)));
        }
    }
}
