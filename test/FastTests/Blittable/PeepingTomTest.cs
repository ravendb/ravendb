using System;
using System.IO;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable
{
    public class PeepingTomTest
    {
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
            using (var context = JsonOperationContext.ShortTermSingleUse())
                PeepingTomStreamTest(originalSize, chunkSizeToRead, offset, context);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void PeepingTomStreamShouldPeepCorrectlyWithRandomValues(int seed)
        {
            var random = new Random(seed);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 10; i++)
                {
                    var originalSize = random.Next(0, 128 * 1024);
                    var chunkSizeToRead = random.Next(0, originalSize);
                    var offset = chunkSizeToRead / 4;

                    PeepingTomStreamTest(originalSize, chunkSizeToRead, offset, context);
                }
            }
        }

        public void PeepingTomStreamTest(int originalSize, int chunkSizeToRead, int offset, JsonOperationContext context)
        {
            var buffer = new byte[originalSize + offset];

            var bytes = new byte[originalSize];
            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = (byte)((i % 26) + 'a');
            }

            using (var stream = new MemoryStream())
            {
                var peeping = new PeepingTomStream(stream, context);
                stream.Write(bytes, 0, originalSize);
                stream.Flush();
                stream.Position = 0;

                var totalRead = 0;
                do
                {
                    int read;
                    do
                    {
                        read = peeping.Read(buffer, offset, chunkSizeToRead);
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

                Assert.Equal(expectedLength, length);

                for (var i = 0; i < peepWindow.Length; i++)
                {
                    var expectedByte = (byte)(((originalSize - peepWindow.Length + i) % 26) + 'a');
                    if (expectedByte != peepWindow[i])
                    {
                        Assert.Equal(expectedByte, peepWindow[i]);
                    }
                }
            }
        }
    }
}
