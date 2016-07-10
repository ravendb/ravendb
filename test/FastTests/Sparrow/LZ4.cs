using Sparrow.Compression;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace FastTests.Sparrow
{
    public unsafe class LZ4Tests
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
            int threshold = 1 << bits;
            var rnd = new Random(size * bits);

            byte[] input = new byte[size];
            for (int i = 0; i < size; i++)
                input[i] = (byte)(rnd.Next() % threshold);

            LZ4 lz4 = new LZ4();

            var maximumOutputLength = LZ4.MaximumOutputLength(input.Length);
            byte* encodeOutput = (byte*)Marshal.AllocHGlobal(maximumOutputLength);

            int compressedSize = 0;
            fixed (byte* pb = input)
            {
                compressedSize = lz4.Encode64(pb, encodeOutput, input.Length, maximumOutputLength);
            }

            byte[] output = new byte[size];
            int uncompressedSize = 0;
            fixed (byte* pb = output)
            {
                uncompressedSize = LZ4.Decode64(encodeOutput, compressedSize, pb, input.Length, true);
            }

            Assert.Equal(input.Length, uncompressedSize);
            for (int i = 0; i < size; i++)
                Assert.Equal(input[i], output[i]);
        
            Marshal.FreeHGlobal((IntPtr)encodeOutput);
        }

        [Fact]
        public void Compress()
        {
            int size = 40;

            LZ4 lz4 = new LZ4();

            byte[] input = new byte[] { 3, 3, 2, 2, 3, 0, 2, 0, 2, 1, 0, 1, 3, 1, 3, 0, 3, 0, 2, 0, 2, 1, 3, 1, 0, 3, 0, 0, 2, 0, 1, 2, 2, 2, 3, 2, 0, 0, 2, 1, 2, 2, 0, 3, 0, 0, 3, 2, 0, 2, 1, 2, 3, 2, 2, 1, 3, 0, 1, 0, 3, 1, 1, 2, 0, 2, 2, 1, 2, 1, 0, 3, 2, 0, 2, 0, 1, 3, 1, 3, 3, 2, 3, 0, 2, 2, 2, 0, 3, 2, 2, 0, 2, 2, 2, 0, 0, 1, 3, 1 };
            byte[] encodedOutput = new byte[LZ4.MaximumOutputLength(input.Length)];

            int compressedSize = 0;
            fixed (byte* inputPtr = input)
            fixed (byte* encodedOutputPtr = encodedOutput)
            {
                compressedSize = lz4.Encode64(inputPtr, encodedOutputPtr, input.Length, encodedOutput.Length);
            }

            byte[] output = new byte[size];
            int uncompressedSize = 0;
            fixed (byte* outputPtr = output)
            fixed (byte* encodedOutputPtr = encodedOutput)
            {
                uncompressedSize = LZ4.Decode64(encodedOutputPtr, compressedSize, outputPtr, input.Length, true);
            }

            Assert.Equal(input.Length, uncompressedSize);
            for (int i = 0; i < size; i++)
            {
                Assert.Equal(input[i], output[i]);
            }
        }        
    }
}
