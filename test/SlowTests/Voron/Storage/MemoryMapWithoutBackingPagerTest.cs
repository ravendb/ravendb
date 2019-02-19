using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests.Voron.Util;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Platform.Win32;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public unsafe class MemoryMapWithoutBackingPagerTest : FastTests.Voron.StorageTest
    {
        private readonly string dummyData;
        private const string LoremIpsum = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
        private const string TestTreeName = "tree";
        private const long PagerInitialSize = 64 * 1024;
        public MemoryMapWithoutBackingPagerTest()
            : base(StorageEnvironmentOptions.CreateMemoryOnly())
        {
            dummyData = GenerateLoremIpsum(1024);
        }

        private string GenerateLoremIpsum(int count)
        {
            return String.Join(Environment.NewLine, Enumerable.Repeat(LoremIpsum, count));
        }

        private IEnumerable<KeyValuePair<string, string>> GenerateTestData()
        {
            for (int i = 0; i < 1000; i++)
                yield return new KeyValuePair<string, string>("Key " + i, "Data:" + dummyData);
        }

        [Fact]
        public void Should_be_able_to_read_and_write_small_values()
        {
            CreatTestSchema();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree(TestTreeName).Add("key", StreamFor("value"));
                tx.Commit();
            }

            using (var snapshot = Env.ReadTransaction())
            {
                var storedValue = Encoding.UTF8.GetString(snapshot.ReadTree(TestTreeName).Read("key").Reader.AsStream().ReadData());
                Assert.Equal("value", storedValue);
            }
        }

        [Theory]
        [InlineData(2)]
        [InlineData(5)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(250)]
        public void Should_be_able_to_allocate_new_pages(int growthMultiplier)
        {
             Env.Options.DataPager.EnsureContinuous(0, growthMultiplier / Constants.Storage.PageSize);
        }

        private void CreatTestSchema()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree(TestTreeName);
                tx.Commit();
            }
        }

        [Fact]
        public void Should_be_able_to_allocate_new_pages_multiple_times()
        {
            var numberOfPages = PagerInitialSize / Constants.Storage.PageSize;
            for (int allocateMorePagesCount = 0; allocateMorePagesCount < 5; allocateMorePagesCount++)
            {
                numberOfPages *= 2;
                Env.Options.DataPager.EnsureContinuous(0, (int)(numberOfPages));
            }
        }

        byte* AllocateMemoryAtEndOfPager(long totalAllocationSize)
        {
            if (StorageEnvironmentOptions.RunningOnPosix)
            {
                var p = Syscall.mmap64(new IntPtr(Env.Options.DataPager.PagerState.MapBase + totalAllocationSize), (UIntPtr)16,
                    MmapProts.PROT_READ | MmapProts.PROT_WRITE, MmapFlags.MAP_ANONYMOUS, -1, 0L);
                if (p.ToInt64() == -1)
                {
                    return null;
                }
                return (byte*)p.ToPointer();
            }
            return Win32MemoryProtectMethods.VirtualAlloc(Env.Options.DataPager.PagerState.MapBase + totalAllocationSize, new UIntPtr(16),
                Win32MemoryProtectMethods.AllocationType.RESERVE, Win32MemoryProtectMethods.MemoryProtection.EXECUTE_READWRITE);
        }

        static void FreeMemoryAtEndOfPager(byte* adjacentBlockAddress)
        {
            if (adjacentBlockAddress == null || adjacentBlockAddress == (byte*)0)
                return;
            if (StorageEnvironmentOptions.RunningOnPosix)
            {
                Syscall.munmap(new IntPtr(adjacentBlockAddress), (UIntPtr)16);
                return;
            }
            Win32MemoryProtectMethods.VirtualFree(adjacentBlockAddress, UIntPtr.Zero, Win32MemoryProtectMethods.FreeType.MEM_RELEASE);
        }

        [Fact]
        public void Should_be_able_to_allocate_new_pages_with_remapping()
        {
            var pagerSize = PagerInitialSize;

            //first grow several times the pager
            for (int allocateMorePagesCount = 0; allocateMorePagesCount < 2; allocateMorePagesCount++)
            {
                pagerSize *= 2;
                Env.Options.DataPager.EnsureContinuous(0, (int)pagerSize);
            }

            var totalAllocationSize = Env.Options.DataPager.PagerState.AllocationInfos.Sum(info => info.Size);

            //prevent continuous allocation and force remapping on next pager growth			
            byte* adjacentBlockAddress = null;
            try
            {
                //if this fails and adjacentBlockAddress == 0 or null --> this means the remapping will occur anyway. 
                //the allocation is here to make sure the remapping does happen in any case
                adjacentBlockAddress = AllocateMemoryAtEndOfPager(totalAllocationSize);

                pagerSize *= 2;
                Env.Options.DataPager.EnsureContinuous(0, (int)(pagerSize / Constants.Storage.PageSize));

            }
            finally
            {
                FreeMemoryAtEndOfPager(adjacentBlockAddress);
            }
        }
    }
}
