using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Voron.Impl.FreeSpace;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Trees
{
    public class FreeSpaceTest : StorageTest
    {
        [PrefixesFact]
        public void WillBeReused()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 25; i++)
                {
                    tx.Root.Add			(i.ToString("0000"), new MemoryStream(buffer));
                }

                tx.Commit();
            }
            var before = Env.Stats();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 25; i++)
                {
                    tx.Root.Delete(i.ToString("0000"));
                }

                tx.Commit();
            }

            var old = Env.NextPageNumber;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 25; i++)
                {
                    tx.Root.Add			(i.ToString("0000"), new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var after = Env.Stats();

            Assert.Equal(after.RootPages, before.RootPages);

            Assert.True(Env.NextPageNumber - old < 2, "This test will not pass until we finish merging the free space branch");
        }

        [PrefixesFact]
        public void ShouldReturnProperPageFromSecondSection()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.FreeSpaceHandling.FreePage(tx, FreeSpaceHandling.NumberOfPagesInSection + 1);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Assert.Equal(FreeSpaceHandling.NumberOfPagesInSection + 1, Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx, 1));
            }
        }

        [PrefixesFact]
        public void CanReuseMostOfFreePages_RemainingOnesCanBeTakenToHandleFreeSpace()
        {
            const int maxPageNumber = 4000000;
            const int numberOfFreedPages = 100;
            var random = new Random(3);
            var freedPages = new HashSet<long>();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.State.NextPageNumber = maxPageNumber + 1;

                tx.Commit();
            }

            for (int i = 0; i < numberOfFreedPages; i++)
            {
                long pageToFree;
                do
                {
                    pageToFree = random.Next(0, maxPageNumber);
                } while (freedPages.Add(pageToFree) == false);

                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    Env.FreeSpaceHandling.FreePage(tx, pageToFree);

                    tx.Commit();
                }
            }

            // we cannot expect that all freed pages will be available for a reuse
            // some freed pages can be used internally by free space handling
            // 80% should be definitely a safe value

            var minNumberOfFreePages = numberOfFreedPages * 0.8;

            for (int i = 0; i < minNumberOfFreePages; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var page = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx, 1);

                    Assert.NotNull(page);
                    Assert.True(freedPages.Remove(page.Value));

                    tx.Commit();
                }
            }
        }

        [PrefixesTheory]
        [InlineData(60, 2)]
        [InlineData(60, 893)]
        [InlineData(60, 6430)]
        [InlineData(60, 7749)]
        [InlineData(56, 893)]
        [InlineDataWithRandomSeed(60)]
        [InlineDataWithRandomSeed(-1)] // also random 'numberOfFreedPages'
        public void FreeSpaceHandlingShouldNotReturnPagesThatAreAlreadyAllocated(int numberOfFreedPages, int seed)
        {
            const int maxPageNumber = 400000;
            var random = new Random(seed);
            var freedPages = new HashSet<long>();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.State.NextPageNumber = maxPageNumber + 1;

                tx.Commit();
            }

            if (numberOfFreedPages == -1)
            {
                numberOfFreedPages = random.Next(0, 10000);
            }

            for (int i = 0; i < numberOfFreedPages; i++)
            {
                long pageToFree;
                do
                {
                    pageToFree = random.Next(0, maxPageNumber);
                } while (freedPages.Add(pageToFree) == false);

                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    Env.FreeSpaceHandling.FreePage(tx, pageToFree);

                    tx.Commit();
                }
            }

            var alreadyReused = new List<long>();
            var freedInternallyByFreeSpaceHandling = new HashSet<long>();

            ((FreeSpaceHandling)Env.FreeSpaceHandling).PageFreed += pageNumber => freedInternallyByFreeSpaceHandling.Add(pageNumber); // need to take into account pages freed by free space handling itself
            do
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var page = Env.FreeSpaceHandling.TryAllocateFromFreeSpace(tx, 1);

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

        [PrefixesFact]
        public void CanGetListOfAllFreedPages()
        {
            const int maxPageNumber = 10000;
            const int numberOfFreedPages = 5000;
            var random = new Random();
            var freedPages = new HashSet<long>();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.State.NextPageNumber = maxPageNumber + 1;

                tx.Commit();
            }

            for (int i = 0; i < numberOfFreedPages; i++)
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    long pageToFree;

                    do
                    {
                        pageToFree = random.Next(0, maxPageNumber);

                        if (tx.Root.AllPages().Contains(pageToFree))
                            continue;

                        if (tx.FreeSpaceRoot.AllPages().Contains(pageToFree))
                            continue;

                        if (freedPages.Add(pageToFree))
                            break;

                    } while (true);

                    Env.FreeSpaceHandling.FreePage(tx, pageToFree);

                    tx.Commit();
                }
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var retrievedFreePages = Env.AllPages(tx)["Free Pages"];

                freedPages.ExceptWith(tx.FreeSpaceRoot.AllPages()); // need to take into account that some of free pages might be used for free space handling
                var sorted = freedPages.OrderBy(x => x);

                Assert.Equal(sorted, retrievedFreePages);
            }
        }
    }
}
