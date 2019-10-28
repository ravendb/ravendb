using System;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
using FastTests.Voron.Util;
using Raven.Client.Documents.Operations.Attachments;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Utils
{
    public class LimitedStreamTests : NoDisposalNeeded
    {
        public LimitedStreamTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void Should_properly_read_ranges(int seed)
        {
            var r = new Random(seed);

            var bytes = new byte[r.Next(128, 1024 * 1024 * 3)];
            r.NextBytes(bytes);

            var ms = new MemoryStream(bytes);
            var max = r.Next(1, bytes.Length / 2);
            var entireStream = new LimitedStream(ms, ms.Length, 0, 0);
            Assert.Equal(bytes, entireStream.ReadData());

            ms.Position = 0;
            long overallRead = 0;
            long position = 0;

            var numberOfChunks = ms.Length / max + (ms.Length % max != 0 ? 1 : 0);

            for (int i = 0; i < numberOfChunks; i++)
            {
                var pos = ms.Position;
                var prev = Math.Min(pos + max, ms.Length);

                var ls = new LimitedStream(ms, prev - pos, position, overallRead);

                if (i == numberOfChunks - 1)
                {
                    Assert.Equal(ms.Length % max, ls.Length);
                }
                else
                {
                    Assert.Equal(max, ls.Length);
                }

                var read = ls.ReadData();

                Assert.Equal(ls.Length, read.Length);
                Assert.Equal(bytes.Skip((int)pos).Take(read.Length).ToArray(), read);

                overallRead += read.Length;
                position += prev - pos;
            }
        }
    }
}
