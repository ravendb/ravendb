using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Pagers
	{
#if DEBUG_PAGER_STATE
//        [Fact]
//        public void PureMemoryPagerReleasesPagerState()
//        {
//            PagerReleasesPagerState(() => new Win32PureMemoryPager());
//        }

        [Fact]
        public void MemoryMapPagerReleasesPagerState()
        {
            PagerReleasesPagerState(() => new Win32MemoryMapPager("db.voron"));
            File.Delete("db.voron");
        }

		[Fact]	
	    public void MemoryMapWithoutBackingReleasePagerState()
	    {
		    PagerReleasesPagerState(() => new Win32PageFileBackedMemoryMappedPager("test"));
	    }

        [Fact]
        public void FilePagerReleasesPagerState()
        {
            PagerReleasesPagerState(() => new FilePager("db.voron"));
            File.Delete("db.voron");
        }

        private static void PagerReleasesPagerState(Func<AbstractPager> constructor)
        {
            var instanceCount = PagerState.Instances.Count;

            using (constructor()) { }

            Assert.Equal(instanceCount, PagerState.Instances.Count);
        }
#endif
	}
}