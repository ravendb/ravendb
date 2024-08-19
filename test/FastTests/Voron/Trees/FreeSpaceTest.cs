using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
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
                tx.LowLevelTransaction.ForTestingPurposesOnly().SetLocalTxNextPageNumber(maxPageNumber + 1 - Env.CurrentStateRecord.NextPageNumber);

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
                tx.LowLevelTransaction.ForTestingPurposesOnly().SetLocalTxNextPageNumber(maxPageNumber + 1 - Env.CurrentStateRecord.NextPageNumber);

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

                freedPages.ExceptWith(Env.FreeSpaceHandling.GetFreePagesOverheadPages(tx.LowLevelTransaction)); // need to take into account that some of free pages might be used for free space handling
                var sorted = freedPages.OrderBy(x => x).ToList();

                Assert.Equal(sorted, retrievedFreePages);
            }
        }
        
         
        [Fact]
        public void ReproduceError()
        {
            var bitArray = new BitArray(1024 * 16);
            using (var tx = Env.WriteTransaction())
            {
                void FreePage(int pageNumber)
                {
                    bitArray[pageNumber] = true;
                    Env.FreeSpaceHandling.FreePage(tx.LowLevelTransaction, pageNumber);
                }

                void TryAllocateFromFreeSpace(int numberOfPages)
                {
                    var match = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx.LowLevelTransaction, numberOfPages);
                    if (match != null)
                    {
                        for (int i = 0; i < numberOfPages; i++)
                        {
                            var pageNum = (int)(i + match.Value);
                            Assert.True(bitArray[pageNum]);
                            bitArray[pageNum] = false;
                        }
                    }
                }
                
                FreePage(3957);
                FreePage(3958);
                FreePage(3959);
                FreePage(3941);
                FreePage(3942);
                FreePage(3943);
                FreePage(3944);
                FreePage(3945);
                FreePage(3946);
                FreePage(3947);
                FreePage(3948);
                FreePage(3949);
                FreePage(3950);
                FreePage(3951);
                FreePage(3952);
                FreePage(3953);
                FreePage(3954);
                FreePage(3955);
                FreePage(3940);
                
                TryAllocateFromFreeSpace(1);
                TryAllocateFromFreeSpace(16);
                TryAllocateFromFreeSpace(256);
                
                FreePage(4229);
                FreePage(4230);
                FreePage(4231);
                FreePage(4213);
                FreePage(4214);
                FreePage(4215);
                FreePage(4216);
                FreePage(4217);
                FreePage(4218);
                FreePage(4219);
                FreePage(4220);
                FreePage(4221);
                FreePage(4222);
                FreePage(4223);
                FreePage(4224);
                FreePage(4225);
                FreePage(4226);
                FreePage(4227);
                FreePage(3940);
                TryAllocateFromFreeSpace(1);
                TryAllocateFromFreeSpace(16);
                TryAllocateFromFreeSpace(256);
            }
        }
    }
}
