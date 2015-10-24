using System.Runtime.CompilerServices;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl.Paging;

namespace Voron
{
    public unsafe class Page
    {
        public readonly byte* Pointer;
        private readonly PageHeader* _header;
        public readonly IVirtualPager Source;

        public Page(byte* pointer, IVirtualPager source)
        {
            Pointer = pointer;
            _header = (PageHeader*) pointer;
            Source = source;
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->PageNumber = value; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int OverflowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->OverflowSize; }
         
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Flags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->Flags = value; }
        }

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