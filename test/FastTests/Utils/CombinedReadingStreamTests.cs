using System;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
using Raven.Server.Utils;
using Xunit;
using Raven.Abstractions.Extensions;

namespace FastTests.Utils
{
    public class CombinedReadingStreamTests
    {
        [Theory]
        [InlineDataWithRandomSeed(3, new [] {1, 7, 5})]
        [InlineDataWithRandomSeed(5, new[] { 12, 34, 19, 1, 12 })]
        [InlineDataWithRandomSeed(6, new[] { 300, 100, 200, 300, 100, 200 })]
        public void CanSeekAndReadFromMultipleStreams(int numberOfBuffers, int[] bufferSizes, int seed)
        {
            var random = new Random(seed);

            var buffers = new byte[numberOfBuffers][];

            for (int i = 0; i < numberOfBuffers; i++)
            {
                buffers[i] = new byte[bufferSizes[i]];
                random.NextBytes(buffers[i]);
            }

            var streams = buffers.Select(x => new MemoryStream(x)).ToArray();

            var totalLength = bufferSizes.Sum();

            var allBytesExpected = buffers.SelectMany(x => x).ToArray();

            var sut = new CombinedReadingStream(streams);
            
            // read all bytes
            var allBytes = sut.ReadEntireBlock(totalLength);
            Assert.Equal(allBytesExpected, allBytes);

            // seek to beginning, read to end
            sut.Position = 0;
            allBytes = sut.ReadEntireBlock(totalLength);
            Assert.Equal(allBytesExpected, allBytes);

            // seek to beginning of last buffer, read to end
            sut.Position -= bufferSizes.Last();
            var result = sut.ReadEntireBlock((int) (totalLength - sut.Position));
            Assert.Equal(buffers.Last(), result);

            // seek to last byte, read it
            sut.Position--;
            var @byte = sut.ReadByte();
            Assert.Equal(buffers.Last().Last(), @byte);

            // seek to 2nd byte of 2nd buffer, read to end
            sut.Position = buffers[0].Length + 1;
            result = sut.ReadEntireBlock(totalLength - buffers[0].Length - 1);
            Assert.Equal(allBytes.Skip(buffers[0].Length + 1).ToArray(), result);

            // seek to beginning read 1st buffer and part of 2nd one
            sut.Position = 0;
            result = new byte[bufferSizes[0] + 1];
            sut.Read(result, 0, result.Length);
            Assert.Equal(buffers[0].Concat(buffers[1].Take(1)).ToArray(), result);

            // seek to beginning
            sut.Position = 0;

            // seek to last byte of last but one buffer, read 4 bytes
            sut.Position = bufferSizes.Last() - 1;
            var pos = (int) sut.Position;
            result = new byte[4];
            sut.Read(result, 0, 4);
            Assert.Equal(allBytes.Skip(pos).Take(4).ToArray(), result);

            // random seeks
            for (int i = 0; i < 100; i++)
            {
                pos = random.Next(0, totalLength - 1);
                sut.Position = pos;

                result = new byte[Math.Min(totalLength - pos, random.Next(1, totalLength))];

                sut.Read(result, 0, result.Length);
                Assert.Equal(allBytes.Skip(pos).Take(result.Length).ToArray(), result);
            }
        }
    }
}