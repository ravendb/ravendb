using System;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
using Raven.Server.Utils;
using Voron.Tests;
using Xunit;

namespace FastTests.Utils
{
    public class LimitedStreamTests : NoDisposalNeeded
    {
        [Theory]
        [InlineDataWithRandomSeed]
        public void Should_properly_read_ranges(int seed)
        {
            var r = new Random(seed);

            var bytes = new byte[r.Next(128, 1024 * 1024 * 3)];
            r.NextBytes(bytes);

            var ms = new MemoryStream(bytes);

            var max = r.Next(1, bytes.Length / 2);

            var entireStream = new LimitedStream(ms, 0, ms.Length);
            Assert.Equal(bytes, entireStream.ReadData());

            ms.Position = 0;

            var numberOfChunks = ms.Length / max + (ms.Length % max != 0 ? 1 : 0);

            for (int i = 0; i < numberOfChunks; i++)
            {
                var pos = (int) ms.Position;

                var ls = new LimitedStream(ms, pos, Math.Min(pos + max, ms.Length));

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
                Assert.Equal(bytes.Skip(pos).Take(read.Length).ToArray(), read);
            }
        }
    }
}