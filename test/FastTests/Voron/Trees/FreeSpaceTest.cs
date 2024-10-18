using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Xunit;
using Voron.Impl.FreeSpace;
using Xunit.Abstractions;

namespace FastTests.Voron.Trees
{
    public class FreeSpaceTest : StorageTest
    {
        public FreeSpaceTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillBeReused()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 25; i++)
                {
                    tree.Add(i.ToString("0000"), new MemoryStream(buffer));
                }

                tx.Commit();
            }
            var before = Env.Stats();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                for (int i = 0; i < 25; i++)
                {
                    tree.Delete(i.ToString("0000"));
                }

                tx.Commit();
            }

            var old = Env.NextPageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                for (int i = 0; i < 25; i++)
                {
                    tree.Add(i.ToString("0000"), new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var after = Env.Stats();

            Assert.Equal(after.RootPages, before.RootPages);

            Assert.True(Env.NextPageNumber - old < 2, "This test will not pass until we finish merging the free space branch");
        }

        [Fact]
        public void ShouldReturnProperPageFromSecondSection()
        {
            using (var tx = Env.WriteTransaction())
            {
                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, FreeSpaceHandling.NumberOfPagesInSection + 1);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(FreeSpaceHandling.NumberOfPagesInSection + 1, Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx.LowLevelTransaction, 1));
            }
        }

        [Theory]
        [InlineData(400, 10, 3)]
        [InlineDataWithRandomSeed(400, 10)]
        public void CanReuseMostOfFreePages_RemainingOnesCanBeTakenToHandleFreeSpace(int maxPageNumber, int numberOfFreedPages, int seed)
        {
            var random = new Random(seed);
            var freedPages = new HashSet<long>();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.State.UpdateNextPage(maxPageNumber + 1);

                tx.Commit();
            }

            for (int i = 0; i < numberOfFreedPages; i++)
            {
                long pageToFree;
                do
                {
                    pageToFree = random.Next(0, maxPageNumber);
                } while (freedPages.Add(pageToFree) == false);

                using (var tx = Env.WriteTransaction())
                {
                    Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, pageToFree);

                    tx.Commit();
                }
            }

            // we cannot expect that all freed pages will be available for a reuse
            // some freed pages can be used internally by free space handling
            // 80% should be definitely a safe value

            var minNumberOfFreePages = numberOfFreedPages * 0.8;

            for (int i = 0; i < minNumberOfFreePages; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var page = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx.LowLevelTransaction, 1);

                    Assert.NotNull(page);
                    Assert.True(freedPages.Remove(page.Value));

                    tx.Commit();
                }
            }
        }

        [Theory]
        [InlineData(400, 6, 2)]
        public void FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated(int maxPageNumber, int numberOfFreedPages, int seed)
        {
            var random = new Random(seed);
            var freedPages = new HashSet<long>();

            if (maxPageNumber == -1)
                maxPageNumber = random.Next(0, 40000);

            if (numberOfFreedPages == -1)
                numberOfFreedPages = random.Next(0, maxPageNumber);

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.State.UpdateNextPage(maxPageNumber + 1);

                tx.Commit();
            }
            
            var freeSpaceHandling = (FreeSpaceHandling) Env.FreeSpaceHandling;
            
            for (int i = 0; i < numberOfFreedPages; i++)
            {
                long pageToFree;
                do
                {
                    pageToFree = random.Next(0, maxPageNumber);
                } while (freedPages.Add(pageToFree) == false);

                using (var tx = Env.WriteTransaction())
                {
                    freeSpaceHandling.FreePage(tx.LowLevelTransaction, pageToFree);

                    tx.Commit();
                }
            }

            var alreadyReused = new List<long>();

            var freedInternallyByFreeSpaceHandling = new HashSet<long>();

            freeSpaceHandling.PageFreed += pageNumber => freedInternallyByFreeSpaceHandling.Add(pageNumber); // need to take into account pages freed by free space handling itself

            do
            {
                using (var tx = Env.WriteTransaction())
                {
                    var page = freeSpaceHandling.TryAllocateFromFreeSpace(tx.LowLevelTransaction, 1);

                    if (page == null)
                    {
                        break;
                    }

                    Assert.False(alreadyReused.Contains(page.Value), "Free space handling returned a page number that has been already allocated. Page number: " + page);
                    Assert.True(freedPages.Remove(page.Value) || freedInternallyByFreeSpaceHandling.Remove(page.Value));

                    alreadyReused.Add(page.Value);

                    tx.Commit();
                }
            } while (true);
        }

        [Theory]
        [InlineDataWithRandomSeed(100, 500)]
        public void CanGetListOfAllFreedPages(int maxPageNumber, int numberOfFreedPages, int seed)
        {
            var random = new Random(seed);
            var freedPages = new HashSet<long>();
            var allocatedPages = new List<long>(maxPageNumber);

            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < maxPageNumber; i++)
                {
                     allocatedPages.Add(tx.LowLevelTransaction.AllocatePage(1).PageNumber);
                }

                tx.Commit();
            }

            for (int i = 0; i < numberOfFreedPages; i++)
            {
                using (var tx = Env.WriteTransaction())
                {

                    do
                    {
                        var idx = random.Next(0, allocatedPages.Count);
                        if (allocatedPages[idx] == -1)
                            break;
                        freedPages.Add(allocatedPages[idx]);
                        Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, allocatedPages[idx]);
                        allocatedPages[idx] = -1;


                    } while (true);


                    tx.Commit();
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                var retrievedFreePages = Env.FreeSpaceHandling.AllPages(tx.LowLevelTransaction);
                var freePagesCount = Env.FreeSpaceHandling.GetFreePagesCount(tx.LowLevelTransaction);
                Assert.Equal(freePagesCount, retrievedFreePages.Count);

                freedPages.ExceptWith(Env.FreeSpaceHandling.GetFreePagesOverheadPages(tx.LowLevelTransaction)); // need to take into account that some of free pages might be used for free space handling
                var sorted = freedPages.OrderBy(x => x).ToList();

                Assert.Equal(sorted, retrievedFreePages);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanUpdateMaxConsecutiveRanges()
        {
            const int totalSections = 4;
            const int totalPages = 2048 * totalSections;

            using (var tx = Env.WriteTransaction())
            {
                var max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);
                Assert.Empty(max);

                for (int i = 0; i < totalPages; i++)
                {
                    tx.LowLevelTransaction.AllocatePage(1);
                }

                tx.Commit();

                max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);
                Assert.Empty(max);
            }

            const int lastPageInAllSections = totalPages - 1;
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < totalPages; i += 2048)
                {
                    Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, i);
                }

                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, lastPageInAllSections);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(3);

                // releasing 2 pages in section 4 to make 3 consecutive pages between two sections
                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, totalPages);
                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, totalPages + 1);
                tx.Commit();

                var max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);

                for (var i = 0; i < totalSections; i++)
                {
                    Assert.Equal(max[i], 3);
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                var page = tx.LowLevelTransaction.AllocatePage(3);
                Assert.Equal(lastPageInAllSections, page.PageNumber);
                tx.Commit();

                var max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);

                for (var i = 0; i < totalSections; i++)
                {
                    Assert.Equal(max[i], 3);
                }

                // shouldn't find anything and update the max consecutive
                tx.LowLevelTransaction.AllocatePage(2);

                max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);

                for (var i = 0; i < totalSections; i++)
                {
                    Assert.Equal(max[i], 2);
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                // releasing from sections 0 and 2 - this should clear the in memory state for those sections
                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, 3);
                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, 2048 * 2 + 15);

                tx.Commit();

                var max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);

                Assert.Equal(max[1], 2);
                Assert.Equal(max[3], 2);
            }

            using (var tx = Env.WriteTransaction())
            {
                Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, 4);

                // not commiting on purpose, this should simulate a rollback and clear the in memory state
            }

            using (var tx = Env.WriteTransaction())
            {
                var max = Env.FreeSpaceHandling.GetMaxConsecutiveRangePerSection(tx.LowLevelTransaction);
                Assert.Empty(max);
            }
        }
    }
}
