using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tests.Infrastructure;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron.Containers
{
    public class ContainerTests(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void CanStoreData()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = (long)Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");

            var id = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), expected.Length, out var space);
            expected.CopyTo(space);

            Container.Get(wtx.LowLevelTransaction, new ContainerEntryId(id), out var actual);

            Assert.Equal(expected.ToArray(), actual.ToSpan().ToArray());
        }


        [RavenFact(RavenTestCategory.Voron)]
        public void CanScanValues()
        {
            var expected = new List<long>();
            long containerId;

            using (var wtx = Env.WriteTransaction())
            {
                containerId = (long)Container.Create(wtx.LowLevelTransaction);

                for (int i = 0; i < 1024; i++)
                {
                    var id = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), 8, out _);
                    expected.Add(id);
                }

                for (int i = 0; i < 16; i++)
                {
                    var id = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), 10_000, out _);
                    expected.Add(id);
                }

                wtx.Commit();
            }

            expected.Sort();

            using (var rtx = Env.ReadTransaction())
            {
                var actual = Container.GetAllIds(rtx.LowLevelTransaction, new ContainerId(containerId));
                actual.Sort();
                Assert.Equal(expected.Count, actual.Count);
                Assert.Equal(expected, actual);
            }
        }


        [RavenFact(RavenTestCategory.Voron)]
        public void CanStoreDeleteAndRecoverSpace()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = (long)Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");
            var ids = new List<long>();

            for (int i = 0; i < 16; i++)
            {
                var id = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), 500, out var s);
                expected.CopyTo(s);
                ids.Add(id);
            }

            for (int i = 0; i < 3; i++)
            {
                Container.Delete(wtx.LowLevelTransaction, new ContainerId(containerId), new ContainerEntryId(ids[^1]));
                ids.RemoveAt(ids.Count - 1);
            }


            for (int i = 0; i < ids.Count; i++)
            {
                Container.Delete(wtx.LowLevelTransaction, new ContainerId(containerId), new ContainerEntryId(ids[i]));
                ids.RemoveAt(i);
            }


            // should not throw here
            Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), 750, out var space);

            foreach (var itemId in ids)
            {
                var data = Container.GetReadOnly(wtx.LowLevelTransaction, new ContainerEntryId(itemId));
                Assert.True(data.Slice(0, 4).SequenceEqual(expected));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanStoreMoreThanASinglePage()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = (long)Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");
            var ids = new List<long>();

            for (int i = 0; i < 16; i++)
            {
                var id = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), 500, out var s);
                expected.CopyTo(s);
                ids.Add(id);
            }

            // should not throw here
            var newId = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), 750, out var space);

            foreach (var itemId in ids)
            {
                var data = Container.GetReadOnly(wtx.LowLevelTransaction, new ContainerEntryId(itemId));
                Assert.True(data.Slice(0, 4).SequenceEqual(expected));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanStoreDeleteAndRecoverSpaceForOverflowPage()
        {
            long containerItemId;
            long containerId;
            long pageId;
            int overflowPageCount;
            const string name = "maciej";

            Span<byte> expected = Encoding.UTF8.GetBytes(string.Join("", Enumerable.Range(0, (10 * 1024) / name.Length).Select(i => name)));

            {
                using var wtx = Env.WriteTransaction();
                containerId = (long)Container.Create(wtx.LowLevelTransaction);
                containerItemId = (long)Container.Allocate(wtx.LowLevelTransaction, new ContainerId(containerId), expected.Length, out var s);
                expected.CopyTo(s);
                wtx.Commit();
            }

            {
                using var wtx = Env.WriteTransaction();
                var container = Container.GetMutable(wtx.LowLevelTransaction, new ContainerEntryId(containerItemId));
                (pageId, _) = Math.DivRem(containerItemId, Constants.Storage.PageSize);
                var page = wtx.LowLevelTransaction.ModifyPage(pageId);
                Assert.True(page.IsOverflow);
                overflowPageCount = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                Container.Delete(wtx.LowLevelTransaction, new ContainerId(containerId), new ContainerEntryId(containerItemId));
                wtx.Commit();
            }

            {
                using var wtx = Env.WriteTransaction();
                var currentAllocations = Env.GetPageOwners(wtx);

                for (long releasedPage = pageId; releasedPage < pageId + overflowPageCount; ++releasedPage)
                {
                    Assert.Contains(releasedPage, currentAllocations.Keys);
                    Assert.Equal(currentAllocations[releasedPage], "Freed Page");
                }

            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanReuseFreeListPageFromPreviousTransaction()
        {
            const int smallSize = 64, bigSize = 4000;
            ContainerId containerId;
            ContainerEntryId first;

            using (var wtx = Env.WriteTransaction())
            {
                var llt = wtx.LowLevelTransaction;
                containerId = Container.Create(llt);
                do
                {
                    first = Container.Allocate(llt, containerId, smallSize, out _);
                }
                while ((long)first / Constants.Storage.PageSize == (long)containerId);

                for (int i = 0; i < 60; i++)
                    Container.Allocate(llt, containerId, smallSize, out _);
                Container.Allocate(llt, containerId, bigSize, out _);
                Container.Allocate(llt, containerId, bigSize, out _);
                wtx.Commit();
            }

            long pageWithSpace = (long)first / Constants.Storage.PageSize;
            bool IsOnFreeList(LowLevelTransaction llt) => Container.GetPagesFor(llt, containerId).FreePages.TryGetValue(pageWithSpace, out _);

            using (var wtx = Env.WriteTransaction())
            {
                Assert.False(IsOnFreeList(wtx.LowLevelTransaction));
                Container.Delete(wtx.LowLevelTransaction, containerId, first);
                wtx.Commit();
            }

            using (var rtx = Env.ReadTransaction())
                Assert.True(IsOnFreeList(rtx.LowLevelTransaction));

            using (var wtx = Env.WriteTransaction())
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, bigSize / 2, out _);
                Assert.Equal(pageWithSpace, (long)id / Constants.Storage.PageSize);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AllocationDoesNotModifyFreeListPagesWithoutEnoughSpace()
        {
            const int itemSize = 64;
            ContainerId containerId;
            var ids = new List<ContainerEntryId>();

            using (var wtx = Env.WriteTransaction())
            {
                containerId = Container.Create(wtx.LowLevelTransaction);
                for (int i = 0; i < 2000; i++)
                    ids.Add(Container.Allocate(wtx.LowLevelTransaction, containerId, itemSize, out _));
                wtx.Commit();
            }

            // half of the items deleted from full pages, so they are on the free list, but their free space isn't in one piece
            var fragmentedPages = new HashSet<long>();
            using (var wtx = Env.WriteTransaction())
            {
                for (int i = 0; i < ids.Count; i += 2)
                {
                    Container.Delete(wtx.LowLevelTransaction, containerId, ids[i]);
                    fragmentedPages.Add((long)ids[i] / Constants.Storage.PageSize);
                }
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var expectedModifiedPages = new HashSet<long> { (long)containerId }; // the root page is modified by every allocation
                for (int i = 0; i < 500; i++)
                    expectedModifiedPages.Add((long)Container.Allocate(wtx.LowLevelTransaction, containerId, itemSize, out _) / Constants.Storage.PageSize);

                fragmentedPages.ExceptWith(expectedModifiedPages);
                Assert.NotEmpty(fragmentedPages);
                Assert.DoesNotContain(fragmentedPages, wtx.LowLevelTransaction.IsDirty);
            }
        }
    }
}
