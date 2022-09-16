using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Compression;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class CompressionTests : NoDisposalNeeded
    {
        public CompressionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ReadAndWriteMaxValues()
        {
            byte* stack = stackalloc byte[16];

            VariableSizeEncoding.Write(stack, byte.MaxValue);
            Assert.Equal(byte.MaxValue, VariableSizeEncoding.Read<byte>(stack, out _));

            VariableSizeEncoding.Write(stack, sbyte.MaxValue);
            Assert.Equal(sbyte.MaxValue, VariableSizeEncoding.Read<sbyte>(stack, out _));

            VariableSizeEncoding.Write(stack, ushort.MaxValue);
            Assert.Equal(ushort.MaxValue, VariableSizeEncoding.Read<ushort>(stack, out _));

            VariableSizeEncoding.Write(stack, short.MaxValue);
            Assert.Equal(short.MaxValue, VariableSizeEncoding.Read<short>(stack, out _));

            VariableSizeEncoding.Write(stack, uint.MaxValue);
            Assert.Equal(uint.MaxValue, VariableSizeEncoding.Read<uint>(stack, out _));

            VariableSizeEncoding.Write(stack, int.MaxValue);
            Assert.Equal(int.MaxValue, VariableSizeEncoding.Read<int>(stack, out _));

            VariableSizeEncoding.Write(stack, ulong.MaxValue);
            Assert.Equal(ulong.MaxValue, VariableSizeEncoding.Read<ulong>(stack, out _));

            VariableSizeEncoding.Write(stack, long.MaxValue);
            Assert.Equal(long.MaxValue, VariableSizeEncoding.Read<long>(stack, out _));
        }

        [Fact]
        public void ReadAndWriteMinValues()
        {
            byte* stack = stackalloc byte[16];

            VariableSizeEncoding.Write(stack, sbyte.MinValue);
            Assert.Equal(sbyte.MinValue, VariableSizeEncoding.Read<sbyte>(stack, out _));

            VariableSizeEncoding.Write(stack, short.MinValue);
            Assert.Equal(short.MinValue, VariableSizeEncoding.Read<short>(stack, out _));

            VariableSizeEncoding.Write(stack, int.MinValue);
            Assert.Equal(int.MinValue, VariableSizeEncoding.Read<int>(stack, out _));

            VariableSizeEncoding.Write(stack, long.MinValue);
            Assert.Equal(long.MinValue, VariableSizeEncoding.Read<long>(stack, out _));
        }

        [Fact]
        public void CorruptedValues()
        {
            byte* stack = stackalloc byte[16];

            int length = VariableSizeEncoding.Write<ushort>(stack, 0xFFFF);
            stack[length - 1] |= 0x80;
            Assert.Throws<FormatException>(() => VariableSizeEncoding.Read<ushort>(stack, out _));

            length = VariableSizeEncoding.Write<uint>(stack, 0xFFFF_FFFF);
            stack[length - 1] |= 0x80;
            Assert.Throws<FormatException>(() => VariableSizeEncoding.Read<uint>(stack, out _));

            length = VariableSizeEncoding.Write<ulong>(stack, 0xFFFF_FFFF_FFFF_FFFF);
            stack[length - 1] |= 0x80;
            Assert.Throws<FormatException>(() => VariableSizeEncoding.Read<ulong>(stack, out _));
        }

        [Theory]
        [InlineData(1337)]
        public void EnsureCompatibility(int seed)
        {
            int ReadVariableSizeInt(byte* buffer, int pos, out int offset)
            {
                offset = 0;

                if (pos < 0)
                    goto ThrowInvalid;

                // Read out an Int32 7 bits at a time.  The high bit 
                // of the byte when on means to continue reading more bytes.
                // we assume that the value shouldn't be zero very often
                // because then we'll always take 5 bytes to store it

                int count = 0;
                byte shift = 0;
                byte b;
                do
                {
                    if (shift == 35)
                        goto Error; // PERF: Using goto to diminish the size of the loop.

                    b = buffer[pos];
                    pos++;
                    offset++;

                    count |= (b & 0x7F) << shift;
                    shift += 7;
                }
                while ((b & 0x80) != 0);

                return count;

                Error:
                ThrowInvalid:
                throw new Exception();
            }

            byte* stack = stackalloc byte[16];

            for (int i = 0; i < byte.MaxValue; i++)
            {
                VariableSizeEncoding.Write(stack, (byte)i);
                var v1 = ReadVariableSizeInt(stack, 0, out _);
                var v2 = VariableSizeEncoding.Read<byte>(stack, out _);
                Assert.Equal(v1, v2);
            }

            var rnd = new Random(seed);

            for (int i = 0; i < 1000; i++)
            {
                ushort value = (ushort)rnd.Next(ushort.MaxValue);

                VariableSizeEncoding.Write(stack, value);
                var v1 = ReadVariableSizeInt(stack, 0, out _);
                var v2 = VariableSizeEncoding.Read<ushort>(stack, out _);
                Assert.Equal(v1, v2);
            }

            for (int i = 0; i < 1000; i++)
            {
                int value = (int)rnd.Next(int.MaxValue);

                VariableSizeEncoding.Write(stack, value);
                var v1 = ReadVariableSizeInt(stack, 0, out _);
                var v2 = VariableSizeEncoding.Read<int>(stack, out _);
                Assert.Equal(v1, v2);
            }
        }
    }
}
