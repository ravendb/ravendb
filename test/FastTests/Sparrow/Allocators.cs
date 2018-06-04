using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public class AllocatorsTests : NoDisposalNeeded
    {
        [Fact]
        public void Alloc_NativeDefault()
        {
            var allocator = new Allocator<NativeBlockAllocator<NativeBlockAllocator.DefaultOptions>>();
            allocator.Initialize(default(NativeBlockAllocator.DefaultOptions));

            allocator.Allocate(1000);
        }
    }
}
