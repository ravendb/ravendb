using System;
using System.IO;
using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class Pagers
    {
#if DEBUG_PAGER_STATE
//        [RavenFact(RavenTestCategory.Voron)]
//        public void PureMemoryPagerReleasesPagerState()
//        {
//            PagerReleasesPagerState(() => new Win32PureMemoryPager());
//        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MemoryMapPagerReleasesPagerState()
        {
            PagerReleasesPagerState(() => new Win32MemoryMapPager("db.voron"));
            File.Delete("db.voron");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MemoryMapWithoutBackingReleasePagerState()
        {
            PagerReleasesPagerState(() => new Win32PageFileBackedMemoryMappedPager("test"));
        }

        [RavenFact(RavenTestCategory.Voron)]
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
