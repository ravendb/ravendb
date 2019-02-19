using Sparrow.Compression;
using System;
using System.Collections.Generic;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class LZ4Tests : NoDisposalNeeded
    {
        public static IEnumerable<object[]> Sizes
        {
            get
            {
                return new[]
                {
                    new object[] { 10, 2 },
                    new object[] { 10, 3 },
                    new object[] { 10, 4 },
                    new object[] { 15, 2 },
                    new object[] { 16, 2 },
                    new object[] { 40, 2 },
                    new object[] { 100, 2 },
                    new object[] { 100, 4 },
                    new object[] { 1000, 4 },
                    new object[] { 10000, 4 },
                    new object[] { 50000, 4 },
                    new object[] { 100000, 4 },
                    new object[] { 100000, 7 }
                };
            }
        }

        [Theory, MemberData("Sizes")]
        public void CompressAndDecompress(int size, int bits)
        {
            uint marker = 0xDEADBEEF;

            int threshold = 1 << bits;
            var rnd = new Random(size * bits);

            byte[] input = new byte[size];
            for (int i = 0; i < size; i++)
                input[i] = (byte)(rnd.Next() % threshold);

            var maximumOutputLength = LZ4.MaximumOutputLength(input.Length);
            byte[] output = new byte[size + sizeof(uint)];
            byte[] encodeOutput = new byte[maximumOutputLength + sizeof(uint)];

            fixed (byte* inputPtr = input)
            fixed (byte* encodedOutputPtr = encodeOutput)
            fixed (byte* outputPtr = output)
            {
                // Setup buffer overrun markers.
                *(uint*)(encodedOutputPtr + maximumOutputLength) = marker;
                *(uint*)(outputPtr + size) = marker;

                int compressedSize = LZ4.Encode64(inputPtr, encodedOutputPtr, size, (int)maximumOutputLength);
                int uncompressedSize = LZ4.Decode64(encodedOutputPtr, compressedSize, outputPtr, size, true);

                Assert.True(compressedSize <= maximumOutputLength);
                Assert.Equal(size, uncompressedSize);
                for (int i = 0; i < size; i++)
                    Assert.Equal(input[i], output[i]);

                // Check buffer overruns didn't happen.
                Assert.Equal(marker, *(uint*)(encodedOutputPtr + maximumOutputLength));
                Assert.Equal(marker, *(uint*)(outputPtr + size));
            }
        }

        [Fact]
        public void Compress2()
        {
            byte[] input = new byte[] { 3, 3, 2, 2, 3, 0, 2, 0, 2, 1, 0, 1, 3, 1, 3, 0, 3, 0, 2, 0, 2, 1, 3, 1, 0, 3, 0, 0, 2, 0, 1, 2, 2, 2, 3, 2, 0, 0, 2, 1, 2, 2, 0, 3, 0, 0, 3, 2, 0, 2, 1, 2, 3, 2, 2, 1, 3, 0, 1, 0, 3, 1, 1, 2, 0, 2, 2, 1, 2, 1, 0, 3, 2, 0, 2, 0, 1, 3, 1, 3, 3, 2, 3, 0, 2, 2, 2, 0, 3, 2, 2, 0, 2, 2, 2, 0, 0, 1, 3, 1 };
            byte[] encodedOutput = new byte[LZ4.MaximumOutputLength(input.Length)];

            int compressedSize = 0;
            fixed (byte* inputPtr = input)
            fixed (byte* encodedOutputPtr = encodedOutput)
            {
                compressedSize = LZ4.Encode64(inputPtr, encodedOutputPtr, input.Length, encodedOutput.Length, 1);
            }

            byte[] output = new byte[input.Length];
            int uncompressedSize = 0;
            fixed (byte* outputPtr = output)
            fixed (byte* encodedOutputPtr = encodedOutput)
            {
                uncompressedSize = LZ4.Decode64(encodedOutputPtr, compressedSize, outputPtr, input.Length, true);
            }

            Assert.Equal(input.Length, uncompressedSize);
            for (int i = 0; i < input.Length; i++)
            {
                Assert.Equal(input[i], output[i]);
            }
        }

        [Fact]
        public void Compress()
        {
            uint marker = 0xDEADBEEF;

            byte[] input = new byte[] { 3, 3, 2, 2, 3, 0, 2, 0, 2, 1, 0, 1, 3, 1, 3, 0, 3, 0, 2, 0, 2, 1, 3, 1, 0, 3, 0, 0, 2, 0, 1, 2, 2, 2, 3, 2, 0, 0, 2, 1, 2, 2, 0, 3, 0, 0, 3, 2, 0, 2, 1, 2, 3, 2, 2, 1, 3, 0, 1, 0, 3, 1, 1, 2, 0, 2, 2, 1, 2, 1, 0, 3, 2, 0, 2, 0, 1, 3, 1, 3, 3, 2, 3, 0, 2, 2, 2, 0, 3, 2, 2, 0, 2, 2, 2, 0, 0, 1, 3, 1 };
            var maximumOutputLength = LZ4.MaximumOutputLength(input.Length);
            byte[] encodedOutput = new byte[maximumOutputLength + sizeof(uint)];
            byte[] output = new byte[input.Length + sizeof(uint)];

            fixed (byte* inputPtr = input)
            fixed (byte* encodedOutputPtr = encodedOutput)
            fixed (byte* outputPtr = output)
            {
                // Setup buffer overrun markers.
                *(uint*)(encodedOutputPtr + maximumOutputLength) = marker;
                *(uint*)(outputPtr + input.Length) = marker;

                int compressedSize = LZ4.Encode64(inputPtr, encodedOutputPtr, input.Length, (int)maximumOutputLength);
                int uncompressedSize = LZ4.Decode64(encodedOutputPtr, compressedSize, outputPtr, input.Length, true);

                Assert.True(compressedSize <= maximumOutputLength);
                Assert.Equal(input.Length, uncompressedSize);
                for (int i = 0; i < input.Length; i++)
                    Assert.Equal(input[i], output[i]);

                // Check buffer overruns didn't happen.
                Assert.Equal(marker, *(uint*)(encodedOutputPtr + maximumOutputLength));
                Assert.Equal(marker, *(uint*)(outputPtr + input.Length));
            }
        }
    }
}
