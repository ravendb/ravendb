using System;
using FastTests.Voron;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_13130 : StorageTest
    {
        [Fact]
        public void Should_throw_on_attempt_to_free_page_which_was_not_allocated_by_NewPageAllocator()
        {
            using (var tx = Env.WriteTransaction())
            {
                var parent = tx.CreateTree("parent");

                var allocator = new NewPageAllocator(tx.LowLevelTransaction, parent);
                allocator.Create();

                var pageAllocatedDirectly = tx.LowLevelTransaction.AllocatePage(1);

                Assert.Throws<InvalidOperationException>(() => allocator.FreePage(pageAllocatedDirectly.PageNumber));
            }
        }
    }
}
