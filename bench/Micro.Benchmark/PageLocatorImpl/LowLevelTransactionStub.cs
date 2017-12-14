using System.Runtime.CompilerServices;

namespace Micro.Benchmark.PageLocatorImpl
{
    public class LowLevelTransactionStub
    {
        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPage ModifyPage(long pageNumber)
        {
            unsafe
            {
                return new MyPage { PageNumber = pageNumber };
            }
        }

        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPage GetPage(long pageNumber)
        {
            unsafe
            {
                return new MyPage { PageNumber = pageNumber };
            }
        }

        [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
        public static MyPageStruct ModifyPageStruct(long pageNumber)
        {
            unsafe
            {
                return new MyPageStruct { PageNumber = pageNumber };
            }
        }

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