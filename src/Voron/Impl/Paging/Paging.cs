using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Voron.Global;

namespace Voron.Impl.Paging
{
    public static unsafe class Paging
    {        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetNumberOfOverflowPages(long overflowSize)
        {
            overflowSize += Constants.Tree.PageHeaderSize;
            return (int)(overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetNumberOfPages(PageHeader* header)
        {
            if ((header->Flags & PageFlags.Overflow) != PageFlags.Overflow)
                return 1;

            var overflowSize = header->OverflowSize + Constants.Tree.PageHeaderSize;
            return checked((overflowSize / Constants.Storage.PageSize) + (overflowSize % Constants.Storage.PageSize == 0 ? 0 : 1));
        }
    }
}
