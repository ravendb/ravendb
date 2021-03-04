using System;
using System.IO;
using System.Threading;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class PeepingTomTest : NoDisposalNeeded
    {
        public PeepingTomTest(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(123456, 65535, 0)]
        [InlineData(1234, 535, 0)]
        [InlineData(4096, 4096, 0)]
        [InlineData(9123, 1024, 0)]
        [InlineData(123456, 65535, 1024)]
        [InlineData(1234, 535, 300)]
        [InlineData(4096, 4096, 4095)]
        [InlineData(9123, 1024, 1024)]
        [InlineData(0, 0, 0)]
        public void PeepingTomStreamShouldPeepCorrectly(int originalSize, int chunkSizeToRead, int offset)
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            using (var context = JsonOperationContext.ShortTermSingleUse())
                PeepingTomStreamTest(originalSize, chunkSizeToRead, offset, seed: null, context, cts.Token);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        [InlineData(1291481720)]
        [InlineData(916490010)]
        public void PeepingTomStreamShouldPeepCorrectlyWithRandomValues(int seed)
        {
            var random = new Random(seed);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 10; i++)
                {
                    var originalSize = random.Next(0, 128 * 1024);
                    var chunkSizeToRead = random.Next(1, originalSize);
                    var offset = chunkSizeToRead / 4;
                    PeepingTomStreamTest(originalSize, chunkSizeToRead, offset, seed, context, cts.Token);
                }
            }
        }

        private static void PeepingTomStreamTest(int originalSize, int chunkSizeToRead, int offset, int? seed, JsonOperationContext context, CancellationToken token)
        {
            try
            {
                var bytes = new byte[originalSize];
                for (var i = 0; i < bytes.Length; i++)
                {
                    bytes[i] = (byte)((i % 26) + 'a');
                }

                using (var stream = new MemoryStream())
                using (context.GetMemoryBuffer(out var memoryBuffer))
                {
                    // fill the buffer with garbage
                    var random = seed == null ? new Random() : new Random(seed.Value);
                    random.NextBytes(memoryBuffer.Memory.Memory.Span);

                    using (var peeping = new PeepingTomStream(stream, memoryBuffer))
                    {
                        stream.Write(bytes, 0, originalSize);
                        stream.Flush();
                        stream.Position = 0;

                        var totalRead = 0;
                        do
                        {
                            token.ThrowIfCancellationRequested();

                            int read = -1;
                            do
                            {
                                token.ThrowIfCancellationRequested();

                                var buffer = new Span<byte>(new byte[originalSize + offset]);
                                read = peeping.Read(buffer.Slice(offset, chunkSizeToRead));
                                totalRead += read;
                                Assert.True(read <= chunkSizeToRead);
                            } while (read != 0);
                        } while (totalRead < originalSize);

                        Assert.Equal(originalSize, totalRead);

                        var peepWindow = peeping.PeepInReadStream();

                        var length = peepWindow.Length;

                        Assert.True(length <= PeepingTomStream.BufferWindowSize);
                        Assert.True(length >= 0);

                        var expectedLength = originalSize < PeepingTomStream.BufferWindowSize ? originalSize : PeepingTomStream.BufferWindowSize;

                        if (expectedLength != length)
                        {
                            var expected = System.Text.Encoding.UTF8.GetString(bytes, bytes.Length - expectedLength, expectedLength);
                            var actual = System.Text.Encoding.UTF8.GetString(peepWindow);
                            Assert.Equal(expected, actual);
                        }
                        Assert.Equal(expectedLength, length);

                        for (var i = 0; i < peepWindow.Length; i++)
                        {
                            token.ThrowIfCancellationRequested();

                            try
                            {
                                var expectedByte = (byte)(((originalSize - peepWindow.Length + i) % 26) + 'a');
                                if (expectedByte != peepWindow[i])
                                {
                                    Assert.Equal(expectedByte, peepWindow[i]);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failure at index: " + i, e);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed with originalSize {originalSize}, chunkSizeToRead {chunkSizeToRead},  offset {offset}", e);
            }
        }
    }
}
