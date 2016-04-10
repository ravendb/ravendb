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
            Header->NodesPerChunk = (ushort)(Pager.PageMaxSpace / sizeof(PrefixTree.Node));           

            // We need to zero the values for the nodes in order to ensure that we will be getting proper data...
            // We can relax this, but then we will have to remove some runtime checks (assertions). 
            // Profile first, remove if necessary.
            Memory.SetInline(DataPointer, 0x00, Header->NodesPerChunk * sizeof(PrefixTree.Node));

            byte* freeSpace = DataPointer + Header->NodesPerChunk * sizeof(PrefixTree.Node);
            Memory.SetInline(freeSpace, 0xFF, Header->NodesPerChunk / BitVector.BitsPerByte + 1);
        }

        public PrefixTree.Node* GetNodePtr(long offset)
        {
            return (PrefixTree.Node*)(this.DataPointer + offset);
        }

        public PrefixTree.Node* GetNodePtrByIndex(long nodeIndex)
        {
            return (PrefixTree.Node*)(this.DataPointer + (nodeIndex * sizeof(PrefixTree.Node)));
        }

        public long GetDiskPointer(long nodeIndex)
        {
            if (nodeIndex >= Header->NodesPerChunk)
                throw new InvalidOperationException("This shouldnt happen.");

            return PageNumber * this.Pager.PageSize + (nodeIndex * sizeof(PrefixTree.Node));
        }

        public long GetIndexFromDiskPointer(long nodePtr)
        {
            return (nodePtr - (PageNumber * this.Pager.PageSize + sizeof(PrefixTreePageHeader))) / sizeof(PrefixTree.Node);
        }
    }
}
