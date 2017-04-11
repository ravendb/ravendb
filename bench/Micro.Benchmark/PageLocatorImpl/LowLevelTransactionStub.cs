using System.Runtime.CompilerServices;
using Voron;

namespace Regression.PageLocator
{
    public class LowLevelTransactionStub
    {
        // TODO: implement register shuffling here.
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPage ModifyPage(long pageNumber)
        {
            unsafe
            {
                return new MyPage { PageNumber = pageNumber };
            }
        }

        // TODO: implement register shuffling here.
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPage GetPage(long pageNumber)
        {
            unsafe
            {
                return new MyPage { PageNumber = pageNumber };
            }
        }

        // TODO: implement register shuffling here.
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPageStruct ModifyPageStruct(long pageNumber)
        {
            unsafe
            {
                return new MyPageStruct { PageNumber = pageNumber };
            }
        }

        // TODO: implement register shuffling here.
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPageStruct GetPageStruct(long pageNumber)
        {
            unsafe
            {
                return new MyPageStruct { PageNumber = pageNumber };
            }
        }
    }
}