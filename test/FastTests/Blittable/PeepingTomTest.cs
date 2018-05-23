using System;
using System.IO;
using System.Text;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable
{
    public class PeepingTomTest : NoDisposalNeeded
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
            var dumpInfoOnError = new StringBuilder($"(PeepingTomStreamShouldPeepCorrectly with originalSize={originalSize}){Environment.NewLine}"); // Remove this after solving RavenDB-10561

            using (var context = JsonOperationContext.ShortTermSingleUse())
                PeepingTomStreamTest(originalSize, chunkSizeToRead, offset, context, dumpInfoOnError, chunkSizeToRead);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        [InlineData(1291481720)]
        [InlineData(916490010)]
        public void PeepingTomStreamShouldPeepCorrectlyWithRandomValues(int seed)
        {
            var dumpInfoOnError = new StringBuilder($"(PeepingTomStreamShouldPeepCorrectlyWithRandomValues with seed={seed}){Environment.NewLine}"); // Remove this after solving RavenDB-10561
            var random = new Random(seed);

           using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 10; i++)
                {
                    dumpInfoOnError.Append($"[{seed}#1]: {i}{Environment.NewLine}");
                    var originalSize = random.Next(0, 128 * 1024);
                    var chunkSizeToRead = random.Next(0, originalSize);
                    var offset = chunkSizeToRead / 4;
                    PeepingTomStreamTest(originalSize, chunkSizeToRead, offset, context, dumpInfoOnError, seed);
                }
            }
        }

        public void PeepingTomStreamTest(int originalSize, int chunkSizeToRead, int offset, JsonOperationContext context, StringBuilder dumpInfoOnError, int debugId)
        {
            try
            {
                var bytes = new byte[originalSize];
                for (var i = 0; i < bytes.Length; i++)
                {
                    bytes[i] = (byte)((i % 26) + 'a');
                }

                using (var stream = new MemoryStream())
                using (var peeping = new PeepingTomStream(stream, context))
                {
                    long tmpptr;
                    unsafe
                    {
                        tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                    }
                    dumpInfoOnError.Append($"[{debugId}#2]: {peeping._returnedBuffer._buffer.Buffer.Count}, {peeping._returnedBuffer._buffer.Buffer.Offset}, {tmpptr}{Environment.NewLine}");

                    stream.Write(bytes, 0, originalSize);
                    stream.Flush();
                    stream.Position = 0;

                    var totalRead = 0;
                    do
                    {
                        int read = -1;
                        do
                        {
                            unsafe
                            {
                                tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                            }
                            dumpInfoOnError.Append($"[{debugId}#3]:{totalRead}, {read}, {originalSize}, {offset}, {chunkSizeToRead}, {peeping._returnedBuffer._buffer.Buffer.Count}, {peeping._returnedBuffer._buffer.Buffer.Offset}, {tmpptr}{Environment.NewLine}");
                            var buffer = new byte[originalSize + offset];
                            read = peeping.Read(buffer, offset, chunkSizeToRead);
                            totalRead += read;
                            Assert.True(read <= chunkSizeToRead);
                            unsafe
                            {
                                tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                            }
                            dumpInfoOnError.Append($"[{debugId}#4]:{totalRead}, {read}, {originalSize}, {offset}, {chunkSizeToRead}, {peeping._returnedBuffer._buffer.Buffer.Count}, {peeping._returnedBuffer._buffer.Buffer.Offset}, {tmpptr}{Environment.NewLine}");
                        } while (read != 0);

                    } while (totalRead < originalSize);

                    Assert.Equal(originalSize, totalRead);

                    unsafe
                    {
                        tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                    }
                    dumpInfoOnError.Append($"[{debugId}#A]:{tmpptr}{Environment.NewLine}");
                    var peepWindow = peeping.PeepInReadStream();
                    unsafe
                    {
                        tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                    }
                    dumpInfoOnError.Append($"[{debugId}#B]:{tmpptr}{Environment.NewLine}");

                    var tmpString = new StringBuilder("");
                    for (int k = 0; k < peepWindow.Length; k++)
                        tmpString.Append($"{peepWindow[k]}");
                    dumpInfoOnError.Append($"[{debugId}#5]:{peepWindow.Length}, {tmpString}, {totalRead}, {originalSize}, {offset}, {chunkSizeToRead}, {peeping._returnedBuffer._buffer.Buffer.Count}, {peeping._returnedBuffer._buffer.Buffer.Offset}, {tmpptr}{Environment.NewLine}");
                    var length = peepWindow.Length;

                    Assert.True(length <= PeepingTomStream.BufferWindowSize);
                    Assert.True(length >= 0);

                    var expectedLength = originalSize < PeepingTomStream.BufferWindowSize ? originalSize : PeepingTomStream.BufferWindowSize;

                    if (expectedLength != length)
                    {
                        var expected = System.Text.Encoding.UTF8.GetString(bytes, bytes.Length - expectedLength, expectedLength);
                        var actual = System.Text.Encoding.UTF8.GetString(peepWindow);
                        unsafe
                        {
                            tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                        }
                        dumpInfoOnError.Append($"[{debugId}#6]: {expectedLength}, {length}, {bytes.Length}, {tmpptr}{Environment.NewLine}");
                        if (expected.Equals(actual) == false)
                        {
                            unsafe
                            {
                                tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                            }
                            dumpInfoOnError.Append($"[{debugId}#7]: {expected[0]},{expected[1]},{expected[2]},{expected[3]},{actual[0]},{actual[1]},{actual[2]},{actual[3]}, {tmpptr}");
                        }
                        Assert.Equal(expected, actual);
                    }
                    Assert.Equal(expectedLength, length);

                    for (var i = 0; i < peepWindow.Length; i++)
                    {
                        var expectedByte = (byte)(((originalSize - peepWindow.Length + i) % 26) + 'a');
                        if (expectedByte != peepWindow[i])
                        {
                            if (expectedByte != peepWindow[i])
                            {
                                byte[] tmp = new byte[5];
                                for (int j = 0; j < 5; j++)
                                {
                                    tmp[j] = i + j < peepWindow.Length ? peepWindow[i + j] : (byte)'-';
                                }
                                unsafe
                                {
                                    tmpptr = new IntPtr(peeping._returnedBuffer._buffer.Pointer).ToInt64();
                                }
                                dumpInfoOnError.Append($"[{debugId}#8]: {i}, {originalSize}, {peepWindow.Length}, {expectedByte}, {tmp[0]},{tmp[1]},{tmp[2]},{tmp[3]},{tmp[4]}, {tmpptr}{Environment.NewLine}");
                            }
                            Assert.Equal(expectedByte, peepWindow[i]);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed with originalSize {originalSize}, chunkSizeToRead {chunkSizeToRead},  offset {offset}. Additional Info:{dumpInfoOnError} EndOfDump", e);
            }
        }
    }
}
