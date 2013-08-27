using System;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Pagers
    {
#if DEBUG
        [Fact]
        public void PureMemoryPagerReleasesPagerState()
        {
            PagerReleasesPagerState(() => new PureMemoryPager());
        }

        [Fact]
        public void MemoryMapPagerReleasesPagerState()
        {
            PagerReleasesPagerState(() => new MemoryMapPager("test.data"));
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