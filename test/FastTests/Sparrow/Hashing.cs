using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class HashingTests : NoDisposalNeeded
    {
        public HashingTests(ITestOutputHelper output) : base(output)
        {
        }

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
            var r3 = Hashing.XXHash32.Calculate("Public");

            Assert.Equal(r1, r2);
            Assert.Equal(r2, r3);
        }

        [Fact]
        public void Marvin32_UseActualValues()
        {
            byte[] value = { (byte)'A', 0, (byte)'b', 0, (byte)'c', 0, (byte)'d', 0, (byte)'e', 0, (byte)'f', 0, (byte)'g', 0, }; /* "Abcdefg" in UTF-16-LE */

            var r1 = Hashing.Marvin32.Calculate(value);
            var r2 = Hashing.Marvin32.Calculate("Abcdefg");

            Assert.Equal(r1, r2);
        }


        [Fact]
        public void XXHash32_UseLongActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("PublicPublicPublicPublic");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("PublicPublicPublicPublic".ToCharArray()));
            var r3 = Hashing.XXHash32.Calculate("PublicPublicPublicPublic");

            Assert.Equal(r1, r2);
            Assert.Equal(r2, r3);
        }


        [Fact]
        public void Metro128_UseActualValues()
        {
            var r1 = Hashing.Metro128.CalculateRaw("Public");
            var r2 = Hashing.Metro128.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);
        }

        [Fact]
        public unsafe void XXHash32_EqualityImplementationPointerAndSpan()
        {
            var rng = new Random();
            for (int it = 0; it < 1000; it++)
            {
                byte[] values = new byte[it];
                rng.NextBytes(values);

                for (int i = 0; i < 100; i++)
                {
                    fixed (byte* ptr = values)
                    {
                        uint h1 = Hashing.XXHash32.CalculateInline(values.AsSpan());
                        uint h2 = Hashing.XXHash32.CalculateInline(ptr, values.Length);

                        Assert.Equal(h1, h2);
                    }
                }
            }
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
        public void Marvin32()
        {
            byte[] test = { (byte)'A', 0, (byte)'b', 0, (byte)'c', 0, (byte)'d', 0, (byte)'e', 0, (byte)'f', 0, (byte)'g', 0, }; /* "Abcdefg" in UTF-16-LE */
            uint r = Hashing.Marvin32.Calculate(test);
            Assert.Equal(r, 0xba627c81);
        }

        [Fact]
        public unsafe void Marvin32_IntArrayEquivalence()
        {
            int[] test = { 32, 5, 11588, 5 }; /* "Abcdefg" in UTF-16-LE */
            
            fixed (int* ptr = test)
            {
                uint r = Hashing.Marvin32.CalculateInline(test);
                uint x = Hashing.Marvin32.CalculateInline((byte*)ptr, test.Length * sizeof(int));

                Assert.Equal(r, x);
            }                        
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
            int h1 = Hashing.HashCombiner.CombineInline(1991, 13);
            int h2 = Hashing.HashCombiner.CombineInline(1991, 12);
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

                var context = new Hashing.Streamed.XXHash32Context {Seed = seed};
                Hashing.Streamed.XXHash32.BeginProcess(ref context);
                fixed (byte* buffer = values)
                {
                    byte* current = buffer;
                    byte* bEnd = buffer + bufferSize;
                    do
                    {
                        int block = Math.Min(blockSize, (int)(bEnd - current));
                        Hashing.Streamed.XXHash32.Process(ref context, current, block);
                        current += block;
                    }
                    while (current < bEnd);
                }

                iteration++;

                var result = Hashing.Streamed.XXHash32.EndProcess(ref context);
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

                var context = new Hashing.Streamed.XXHash64Context {Seed = seed};
                Hashing.Streamed.XXHash64.BeginProcess(ref context);
                fixed (byte* buffer = values)
                {
                    byte* current = buffer;
                    byte* bEnd = buffer + bufferSize;
                    do
                    {
                        int block = Math.Min(blockSize, (int)(bEnd - current));
                        Hashing.Streamed.XXHash64.Process(ref context, current, block);
                        current += block;
                    }
                    while (current < bEnd);
                }

                iteration++;

                var result = Hashing.Streamed.XXHash64.EndProcess(ref context);
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
