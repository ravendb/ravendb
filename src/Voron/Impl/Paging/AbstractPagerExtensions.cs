using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Voron.Global;

namespace Voron.Impl.Paging
{
    public static unsafe class Pager
    {        
        public const int PageMaxSpace = Constants.Storage.PageSize - Constants.Tree.PageHeaderSize;
        // NodeMaxSize - RequiredSpaceForNewNode for 4Kb page is 2038, so we drop this by a bit
        public const int MaxKeySize = 2038 - RequiredSpaceForNewNode;

        public const int RequiredSpaceForNewNode = Constants.Tree.NodeHeaderSize + Constants.Tree.NodeOffsetSize;

        public const int PageMinSpace = (int)(PageMaxSpace * 0.33);
        public const int NodeMaxSize = PageMaxSpace / 2 - 1;

      
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsKeySizeValid(int keySize)
        {
            if (keySize > MaxKeySize)
                return false;

            return true;
        }
   
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
