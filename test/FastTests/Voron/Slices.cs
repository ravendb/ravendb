using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class SliceTests : NoDisposalNeeded
    {
        public SliceTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BasicContains()
        {
            using ByteStringContext context = new ByteStringContext(SharedMultipleUseFlag.None);

            Slice.From(context, "testing", out var original);
            Slice.From(context, "ing", out var suffix);
            Slice.From(context, "test", out var prefix);
            Slice.From(context, "stin", out var middle);
            Slice.From(context, "testing1", out var tooBig);
            Slice.From(context, "a", out var noMatch);
            Slice.From(context, "tw", out var multipleHits);

            Assert.True(original.Contains(suffix));
            Assert.True(original.Contains(prefix));
            Assert.True(original.Contains(middle));
            Assert.False(original.Contains(tooBig));
            Assert.False(original.Contains(noMatch));
            Assert.True(original.Contains(original));
            Assert.False(original.Contains(multipleHits));
        }
    }
}
