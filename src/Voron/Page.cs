using System.Runtime.CompilerServices;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl.Paging;

namespace Voron
{
    public sealed unsafe class Page
    {
        public readonly byte* Pointer;
        public readonly IVirtualPager Source;

        public Page(byte* pointer, IVirtualPager source)
        {
            Pointer = pointer;
            Source = source;
        }

        public byte* DataPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer + sizeof(PageHeader); }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { ((PageHeader*)Pointer)->PageNumber = value; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (((PageHeader*)Pointer)->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int OverflowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->OverflowSize; }
            set { ((PageHeader*)Pointer)->OverflowSize = value; }
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->Flags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { ((PageHeader*)Pointer)->Flags = value; }
        }

        // TODO: Convert all these methods in explicit casting operators. 
        public TreePage ToTreePage()
        {
            return new TreePage(Pointer, Source.DebugInfo, Source.PageSize);
        }

        public FixedSizeTreePage ToFixedSizeTreePage()
        {
            return new FixedSizeTreePage(Pointer, Source.DebugInfo, Source.PageSize);
        }
    }
}