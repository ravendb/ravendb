using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Pagers
    {
#if DEBUG
	    class MockStorageQuota : IStorageQuotaOptions
	    {
		    public long? MaxStorageSize { get; set; }
		    public long GetCurrentStorageSize()
		    {
			    return -1;
		    }
	    }

        [Fact]
        public void PureMemoryPagerReleasesPagerState()
        {
            PagerReleasesPagerState(() => new PureMemoryPager(new MockStorageQuota()));
        }

        [Fact]
        public void MemoryMapPagerReleasesPagerState()
        {
			PagerReleasesPagerState(() => new MemoryMapPager("db.voron", new MockStorageQuota()));
            File.Delete("db.voron");
        }


        [Fact]
        public void FilePagerReleasesPagerState()
        {
			PagerReleasesPagerState(() => new FilePager("db.voron", new MockStorageQuota()));
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