using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron.FixedSize;
using Raven.Client.Extensions.Streams;
using Raven.Server.Utils;
using Xunit;
using Voron.Util;

namespace FastTests.Utils
{
    public class ChunkedMmapStreamTests : NoDisposalNeeded
    {
        [Theory]
        [InlineDataWithRandomSeed(13, 3)]
        [InlineDataWithRandomSeed(94, 7)]
        public unsafe void Can_seek_and_read_from_chunked_mmap_file(int totalSize, int chunkSize, int seed)
        {
            var random = new Random(seed);
            
            var buffer = new byte[totalSize];
            random.NextBytes(buffer);

            fixed (byte* ptr = buffer)
            {
                var ptrSize = new List<PtrSize>();

                var numberOfBuffers = totalSize / chunkSize + (totalSize % chunkSize != 0 ? 1 : 0);

                for (int i = 0; i < numberOfBuffers; i++)
                {
                    ptrSize.Add(PtrSize.Create(
                        ptr + (i * chunkSize),
                        i == numberOfBuffers - 1 ? totalSize % chunkSize : chunkSize));
                }
                
                var sut = new ChunkedMmapStream(ptrSize.ToArray(), chunkSize);

                // read all bytes
                var allBytes = sut.ReadEntireBlock(totalSize);
                Assert.Equal(buffer, allBytes);

                // seek to beginning, read to end
                sut.Position = 0;
                allBytes = sut.ReadData();
                Assert.Equal(buffer, allBytes);
                
                // read all bytes one by one
                sut.Position = 0;
                for (int i = 0; i < allBytes.Length; i++)
                {
                    Assert.Equal(allBytes[i], sut.ReadByte());
                }

                // random seeks
                for (int i = 0; i < 1000; i++)
                {
                    var pos = random.Next(0, totalSize - 1);
                    sut.Position = pos;

                    var result = new byte[Math.Min(totalSize - pos, random.Next(1, totalSize))];

                    sut.ReadEntireBlock(result, 0, result.Length);
                    Assert.Equal(buffer.Skip(pos).Take(result.Length).ToArray(), result);
                }
            }
        }
    }
}