using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public class HashingTests
    {
        [Fact]
        public void XXHash64_UseActualValues()
        {
            var r1 = Hashing.XXHash64.CalculateRaw("Public");
            var r2 = Hashing.XXHash64.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);
        }

        [Fact]
        public void XXHash32_UseActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("Public");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);
        }


        [Fact]
        public void Metro128_UseActualValues()
        {
            var r1 = Hashing.Metro128.CalculateRaw("Public");
            var r2 = Hashing.Metro128.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);
        }

        [Fact]
        public void XXHash32()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            uint expected = 0xA3643705;
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0x32D153FF;
            Assert.Equal(expected, result);

            value = "heiå";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0xDB5ABCCC;
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 0);
            expected = 0xD855F606;
            Assert.Equal(expected, result);

            value = "asdfasdfasdfasdfasdfasdfasdfuqrewpuqpuqruepoiqiwoerpuqowieruqwlekjrqwernq/wemnrq.,wemrn";
            var bytes = Encoding.UTF8.GetBytes(value);
            result = Hashing.XXHash32.Calculate(bytes);
            expected = 3571373779;
            Assert.Equal(expected, result);
        }


        [Fact]
        public void Metro128()
        {
            var r1 = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes("012345678901234567890123456789012345678901234567890123456789012"), seed: 0);

            Assert.Equal(r1.H1, 0x9B9FEDA4BFE27CC7UL);
            Assert.Equal(r1.H2, 0x97A27450ACB24805UL);
        }


        [Fact]
        public void XXHash32_EquivalenceInDifferentMemoryLocations()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            uint expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void XXHash64_EquivalenceInDifferentMemoryLocationsXXHash64()
        {
            string value = "abcd";
            ulong result = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            ulong expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void Metro128_EquivalenceInDifferentMemoryLocations()
        {
            string value = "abcd";
            var result = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            var expected = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "abc";
            result = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);

            value = "κόσμε";
            result = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            expected = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.Equal(expected, result);
        }


        [Fact]
        public void XXHash32_NotEquivalenceOfBytesWithString()
        {
            string value = "abcd";
            uint result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            uint expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "abc";
            result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash32.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash32.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);
        }


        [Fact]
        public void XXHash64_NotEquivalenceOfBytesWithString()
        {
            string value = "abcd";
            ulong result = Hashing.XXHash64.CalculateRaw(value, seed: 10);
            ulong expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "abc";
            result = Hashing.XXHash64.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "κόσμε";
            result = Hashing.XXHash64.CalculateRaw(value, seed: 10);
            expected = Hashing.XXHash64.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);
        }

        [Fact]
        public void Metro128_NotEquivalenceOfBytesWithString()
        {
            string value = "abcd";
            var result = Hashing.Metro128.CalculateRaw(value, seed: 10);
            var expected = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "abc";
            result = Hashing.Metro128.CalculateRaw(value, seed: 10);
            expected = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);

            value = "κόσμε";
            result = Hashing.Metro128.CalculateRaw(value, seed: 10);
            expected = Hashing.Metro128.Calculate(Encoding.UTF8.GetBytes(value), seed: 10);
            Assert.NotEqual(expected, result);
        }


        public static IEnumerable<object[]> BufferSize
        {
            get
            {
                return new[]
                {
                    new object[] {1},
                    new object[] {4},
                    new object[] {15},
                    new object[] {65},
                    new object[] {90},
                    new object[] {128},
                    new object[] {129},
                    new object[] {1000},
                };
            }
        }

        [Theory]
        [MemberData("BufferSize")]
        public void XXHash32_IterativeHashingEquivalence(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;
            var context = Hashing.Iterative.XXHash32.Preprocess(values, values.Length, seed);

            for (int i = 1; i < values.Length; i++)
            {
                var expected = Hashing.XXHash32.Calculate(values, i, seed);
                var result = Hashing.Iterative.XXHash32.Calculate(values, i, context);
                Assert.Equal(expected, result);
            }
        }

        [Theory]
        [MemberData("BufferSize")]
        public void XXHash32_IterativeHashingPrefixing(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;
            var context = Hashing.Iterative.XXHash32.Preprocess(values, values.Length, seed);

            for (int i = 1; i < values.Length; i++)
            {
                var expected = Hashing.XXHash32.Calculate(values, i, seed);

                for (int j = 0; j < i; j++)
                {
                    var result = Hashing.Iterative.XXHash32.Calculate(values, i, context, j);
                    Assert.Equal(expected, result);
                }
            }
        }

        [Fact]
        public void Combine()
        {
            int h1 = Hashing.CombineInline(1991, 13);
            int h2 = Hashing.CombineInline(1991, 12);
            Assert.NotEqual(h1, h2);
        }


        [Theory]
        [MemberData("BufferSize")]
        public unsafe void XXHash32_StreamedHashingEquivalence(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;

            int blockSize;
            int iteration = 1;
            do
            {
                blockSize = Hashing.Streamed.XXHash32.Alignment * iteration;

                var context = Hashing.Streamed.XXHash32.BeginProcess(seed);
                fixed (byte* buffer = values)
                {
                    byte* current = buffer;
                    byte* bEnd = buffer + bufferSize;
                    do
                    {
                        int block = Math.Min(blockSize, (int)(bEnd - current));
                        context = Hashing.Streamed.XXHash32.Process(context, current, block);
                        current += block;
                    }
                    while (current < bEnd);
                }

                iteration++;

                var result = Hashing.Streamed.XXHash32.EndProcess(context);
                var expected = Hashing.XXHash32.Calculate(values, -1, seed);

                Assert.Equal(expected, result);
            }
            while (blockSize <= bufferSize);
        }

        [Theory]
        [MemberData("BufferSize")]
        public unsafe void XXHash64_StreamedHashingEquivalence(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;

            int blockSize;
            int iteration = 1;
            do
            {
                blockSize = Hashing.Streamed.XXHash64.Alignment * iteration;

                var context = Hashing.Streamed.XXHash64.BeginProcess(seed);
                fixed (byte* buffer = values)
                {
                    byte* current = buffer;
                    byte* bEnd = buffer + bufferSize;
                    do
                    {
                        int block = Math.Min(blockSize, (int)(bEnd - current));
                        context = Hashing.Streamed.XXHash64.Process(context, current, block);
                        current += block;
                    }
                    while (current < bEnd);
                }

                iteration++;

                var result = Hashing.Streamed.XXHash64.EndProcess(context);
                var expected = Hashing.XXHash64.Calculate(values, -1, seed);

                Assert.Equal(expected, result);
            }
            while (blockSize <= bufferSize);
        }

        [Theory]
        [MemberData("BufferSize")]
        public unsafe void Metro128_StreamedHashingEquivalence(int bufferSize)
        {
            var rnd = new Random(1000);

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            uint seed = 233;

            int blockSize;
            int iteration = 1;
            do
            {
                blockSize = Hashing.Streamed.Metro128.Alignment * iteration;

                var context = Hashing.Streamed.Metro128.BeginProcess(seed);
                fixed (byte* buffer = values)
                {
                    byte* current = buffer;
                    byte* bEnd = buffer + bufferSize;
                    do
                    {
                        int block = Math.Min(blockSize, (int)(bEnd - current));
                        context = Hashing.Streamed.Metro128.Process(context, current, block);
                        current += block;
                    }
                    while (current < bEnd);
                }

                iteration++;

                var result = Hashing.Streamed.Metro128.EndProcess(context);
                var expected = Hashing.Metro128.Calculate(values, -1, seed);

                Assert.Equal(expected, result);
            }
            while (blockSize <= bufferSize);
        }
    }
}
