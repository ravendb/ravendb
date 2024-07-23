using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Containers
{
    public class ContainerTests : StorageTest
    {
        public ContainerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStoreData()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");

            var id = Container.Allocate(wtx.LowLevelTransaction, containerId, expected.Length, out var space);
            expected.CopyTo(space);

            Container.Get(wtx.LowLevelTransaction, id, out var actual);

            Assert.Equal(expected.ToArray(), actual.ToSpan().ToArray());
        }


        [Fact]
        public void CanScanValues()
        {
            var expected = new List<long>();
            long containerId;

            using (var wtx = Env.WriteTransaction())
            {
                containerId = Container.Create(wtx.LowLevelTransaction);

                for (int i = 0; i < 1024; i++)
                {
                    var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 8, out _);
                    expected.Add(id);
                }

                for (int i = 0; i < 16; i++)
                {
                    var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 10_000, out _);
                    expected.Add(id);
                }

                wtx.Commit();
            }

            expected.Sort();

            using (var rtx = Env.ReadTransaction())
            {
                var actual = Container.GetAllIds(rtx.LowLevelTransaction, containerId);
                actual.Sort();
                Assert.Equal(expected.Count, actual.Count);
                Assert.Equal(expected, actual);
            }
        }


        [Fact]
        public void CanStoreDeleteAndRecoverSpace()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");
            var ids = new List<long>();

            for (int i = 0; i < 16; i++)
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 500, out var s);
                expected.CopyTo(s);
                ids.Add(id);
            }

            for (int i = 0; i < 3; i++)
            {
                Container.Delete(wtx.LowLevelTransaction, containerId, ids[^1]);
                ids.RemoveAt(ids.Count - 1);
            }


            for (int i = 0; i < ids.Count; i++)
            {
                Container.Delete(wtx.LowLevelTransaction, containerId, ids[i]);
                ids.RemoveAt(i);
            }


            // should not throw here
            Container.Allocate(wtx.LowLevelTransaction, containerId, 750, out var space);

            foreach (var itemId in ids)
            {
                var data = Container.GetReadOnly(wtx.LowLevelTransaction, itemId);
                Assert.True(data.Slice(0, 4).SequenceEqual(expected));
            }
        }

        [Fact]
        public void CanStoreMoreThanASinglePage()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");
            var ids = new List<long>();

            for (int i = 0; i < 16; i++)
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 500, out var s);
                expected.CopyTo(s);
                ids.Add(id);
            }

            // should not throw here
            var newId = Container.Allocate(wtx.LowLevelTransaction, containerId, 750, out var space);

            foreach (var itemId in ids)
            {
                var data = Container.GetReadOnly(wtx.LowLevelTransaction, itemId);
                Assert.True(data.Slice(0, 4).SequenceEqual(expected));
            }
        }

        [Fact]
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
                containerId = Container.Create(wtx.LowLevelTransaction);
                containerItemId = Container.Allocate(wtx.LowLevelTransaction, containerId, expected.Length, out var s);
                expected.CopyTo(s);
                wtx.Commit();
            }

            {
                using var wtx = Env.WriteTransaction();
                var container = Container.GetMutable(wtx.LowLevelTransaction, containerItemId);
                (pageId, _) = Math.DivRem(containerItemId, Constants.Storage.PageSize);
                var page = wtx.LowLevelTransaction.ModifyPage(pageId);
                Assert.True(page.IsOverflow);
                overflowPageCount = Paging.GetNumberOfOverflowPages(page.OverflowSize);
                Container.Delete(wtx.LowLevelTransaction, containerId, containerItemId);
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
    }
}
