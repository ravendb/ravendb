using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class HashingTests : NoDisposalNeeded
    {
        public HashingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash64_UseActualValues()
        {
            var r1 = Hashing.XXHash64.CalculateRaw("Public");
            var r2 = Hashing.XXHash64.CalculateRaw(new string("Public".ToCharArray()));

            Assert.Equal(r1, r2);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32_UseActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("Public");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("Public".ToCharArray()));
            var r3 = Hashing.XXHash32.Calculate("Public");

            Assert.Equal(r1, r2);
            Assert.Equal(r2, r3);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Marvin32_UseActualValues()
        {
            byte[] value = { (byte)'A', 0, (byte)'b', 0, (byte)'c', 0, (byte)'d', 0, (byte)'e', 0, (byte)'f', 0, (byte)'g', 0, }; /* "Abcdefg" in UTF-16-LE */

            var r1 = Hashing.Marvin32.Calculate(value);
            var r2 = Hashing.Marvin32.Calculate("Abcdefg");

            Assert.Equal(r1, r2);
        }


        [RavenFact(RavenTestCategory.Core)]
        public void XXHash32_UseLongActualValues()
        {
            var r1 = Hashing.XXHash32.CalculateRaw("PublicPublicPublicPublic");
            var r2 = Hashing.XXHash32.CalculateRaw(new string("PublicPublicPublicPublic".ToCharArray()));
            var r3 = Hashing.XXHash32.Calculate("PublicPublicPublicPublic");

            Assert.Equal(r1, r2);
            Assert.Equal(r2, r3);
        }


        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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


        [RavenFact(RavenTestCategory.Core)]
        public void Marvin32()
        {
            byte[] test = { (byte)'A', 0, (byte)'b', 0, (byte)'c', 0, (byte)'d', 0, (byte)'e', 0, (byte)'f', 0, (byte)'g', 0, }; /* "Abcdefg" in UTF-16-LE */
            uint r = Hashing.Marvin32.Calculate(test);
            Assert.Equal(r, 0xba627c81);
        }

        [RavenFact(RavenTestCategory.Core)]
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


        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
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


        [RavenFact(RavenTestCategory.Core)]
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

        [RavenFact(RavenTestCategory.Core)]
        public unsafe void EnsureZeroLengthStringIsAValidHash()
        {
            byte[] zeroLength = Array.Empty<byte>();
            byte[] nonZeroLength = "abcd"u8.ToArray();

            fixed (byte* zeroPtr = zeroLength)
            fixed (byte* nonZeroPtr = nonZeroLength)
            {
                uint zeroHash = Hashing.XXHash32.Calculate(zeroLength);
                Assert.Equal(zeroHash, Hashing.XXHash32.Calculate(zeroPtr, 0));
                Assert.Equal(zeroHash, Hashing.XXHash32.Calculate(nonZeroLength, 0));
                Assert.Equal(zeroHash, Hashing.XXHash32.Calculate(nonZeroPtr, 0));

                ulong zeroHashLong = Hashing.XXHash64.Calculate(zeroLength);
                Assert.Equal(zeroHashLong, Hashing.XXHash64.Calculate(zeroPtr, 0));
                Assert.Equal(zeroHashLong, Hashing.XXHash64.Calculate(nonZeroLength, 0));
                Assert.Equal(zeroHashLong, Hashing.XXHash64.Calculate(nonZeroPtr, 0));

                var marvinHash = Hashing.Marvin32.Calculate(zeroLength);
                Assert.Equal(marvinHash, Hashing.Marvin32.CalculateInline(zeroPtr, 0));
                Assert.Equal(marvinHash, Hashing.Marvin32.CalculateInline(nonZeroPtr, 0));
                Assert.Equal(marvinHash, Hashing.Marvin32.CalculateInline(new List<int>()));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Combine()
        {
            int h1 = Hashing.HashCombiner.CombineInline(1991, 13);
            int h2 = Hashing.HashCombiner.CombineInline(1991, 12);
            Assert.NotEqual(h1, h2);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public unsafe void XXHash64_StreamedHashingEquivalence(int randomSeed)
        {
            var rnd = new Random(randomSeed);
            var bufferSize = rnd.Next(1, 1000);
            var seed = (ulong)rnd.Next();

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            int blockSize;
            int iteration = 1;
            do
            {
                blockSize = Hashing.Streamed.XXHash64.Alignment * iteration;

                var context = new Hashing.Streamed.XXHash64Context { Seed = seed };
                Hashing.Streamed.XXHash64.Begin(ref context);
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

                var result = Hashing.Streamed.XXHash64.End(ref context);
                var expected = Hashing.XXHash64.Calculate(values, -1, seed);

                Assert.Equal(expected, result);
            }
            while (blockSize <= bufferSize);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public unsafe void XXHash64_HashingEquivalenceWithReference(int randomSeed)
        {
            var rnd = new Random(randomSeed);
            var bufferSize = rnd.Next(1, 1000);
            var seed = (ulong)rnd.Next();

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            fixed (byte* valuePtr = values)
            {
                for (int i = 1; i < values.Length; i++)
                {
                    var expected = XXHash64Reference(valuePtr, (ulong)i, seed: seed);
                    var result = Hashing.XXHash64.CalculateInline(values.AsSpan().Slice(0, i), seed);

                    Assert.Equal(expected, result);
                }
            }
        }

        private static unsafe ulong XXHash64Reference(byte* buffer, ulong len, ulong seed = 0)
        {
            ulong h64;

            byte* bEnd = buffer + len;

            if (len >= 32)
            {
                byte* limit = bEnd - 32;

                ulong v1 = seed + Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_2;
                ulong v2 = seed + Hashing.XXHash64Constants.PRIME64_2;
                ulong v3 = seed + 0;
                ulong v4 = seed - Hashing.XXHash64Constants.PRIME64_1;

                do
                {
                    v1 += ((ulong*)buffer)[0] * Hashing.XXHash64Constants.PRIME64_2;
                    v2 += ((ulong*)buffer)[1] * Hashing.XXHash64Constants.PRIME64_2;
                    v3 += ((ulong*)buffer)[2] * Hashing.XXHash64Constants.PRIME64_2;
                    v4 += ((ulong*)buffer)[3] * Hashing.XXHash64Constants.PRIME64_2;

                    buffer += 4 * sizeof(ulong);

                    v1 = Bits.RotateLeft64(v1, 31);
                    v2 = Bits.RotateLeft64(v2, 31);
                    v3 = Bits.RotateLeft64(v3, 31);
                    v4 = Bits.RotateLeft64(v4, 31);

                    v1 *= Hashing.XXHash64Constants.PRIME64_1;
                    v2 *= Hashing.XXHash64Constants.PRIME64_1;
                    v3 *= Hashing.XXHash64Constants.PRIME64_1;
                    v4 *= Hashing.XXHash64Constants.PRIME64_1;
                }
                while (buffer <= limit);

                h64 = Bits.RotateLeft64(v1, 1) + Bits.RotateLeft64(v2, 7) + Bits.RotateLeft64(v3, 12) + Bits.RotateLeft64(v4, 18);

                v1 *= Hashing.XXHash64Constants.PRIME64_2;
                v2 *= Hashing.XXHash64Constants.PRIME64_2;
                v3 *= Hashing.XXHash64Constants.PRIME64_2;
                v4 *= Hashing.XXHash64Constants.PRIME64_2;

                v1 = Bits.RotateLeft64(v1, 31);
                v2 = Bits.RotateLeft64(v2, 31);
                v3 = Bits.RotateLeft64(v3, 31);
                v4 = Bits.RotateLeft64(v4, 31);

                v1 *= Hashing.XXHash64Constants.PRIME64_1;
                v2 *= Hashing.XXHash64Constants.PRIME64_1;
                v3 *= Hashing.XXHash64Constants.PRIME64_1;
                v4 *= Hashing.XXHash64Constants.PRIME64_1;

                h64 ^= v1;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v2;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v3;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v4;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;
            }
            else
            {
                h64 = seed + Hashing.XXHash64Constants.PRIME64_5;
            }

            h64 += (ulong)len;


            while (buffer + 8 <= bEnd)
            {
                ulong k1 = *((ulong*)buffer);
                k1 *= Hashing.XXHash64Constants.PRIME64_2;
                k1 = Bits.RotateLeft64(k1, 31);
                k1 *= Hashing.XXHash64Constants.PRIME64_1;
                h64 ^= k1;
                h64 = Bits.RotateLeft64(h64, 27) * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;
                buffer += 8;
            }

            if (buffer + 4 <= bEnd)
            {
                h64 ^= *(uint*)buffer * Hashing.XXHash64Constants.PRIME64_1;
                h64 = Bits.RotateLeft64(h64, 23) * Hashing.XXHash64Constants.PRIME64_2 + Hashing.XXHash64Constants.PRIME64_3;
                buffer += 4;
            }

            while (buffer < bEnd)
            {
                h64 ^= ((ulong)*buffer) * Hashing.XXHash64Constants.PRIME64_5;
                h64 = Bits.RotateLeft64(h64, 11) * Hashing.XXHash64Constants.PRIME64_1;
                buffer++;
            }

            h64 ^= h64 >> 33;
            h64 *= Hashing.XXHash64Constants.PRIME64_2;
            h64 ^= h64 >> 29;
            h64 *= Hashing.XXHash64Constants.PRIME64_3;
            h64 ^= h64 >> 32;

            return h64;
        }


        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public unsafe void XXHash32_HashingEquivalenceWithReference(int randomSeed)
        {
            var rnd = new Random(randomSeed);
            var bufferSize = rnd.Next(1, 1000);
            var seed = (uint)rnd.Next();

            byte[] values = new byte[bufferSize];
            rnd.NextBytes(values);

            fixed (byte* valuePtr = values)
            {
                for (int i = 1; i < values.Length; i++)
                {
                    var expected = XXHash32Reference(valuePtr, i, seed: seed);
                    var result = Hashing.XXHash32.CalculateInline(values.AsSpan().Slice(0, i), seed);

                    Assert.Equal(expected, result);
                }
            }
        }

        private static unsafe uint XXHash32Reference(byte* buffer, int len, uint seed = 0)
        {
            unchecked
            {
                uint h32;

                byte* bEnd = buffer + len;

                if (len >= 16)
                {
                    byte* limit = bEnd - 16;

                    uint v1 = seed + Hashing.XXHash32Constants.PRIME32_1 + Hashing.XXHash32Constants.PRIME32_2;
                    uint v2 = seed + Hashing.XXHash32Constants.PRIME32_2;
                    uint v3 = seed + 0;
                    uint v4 = seed - Hashing.XXHash32Constants.PRIME32_1;

                    do
                    {
                        v1 += ((uint*)buffer)[0] * Hashing.XXHash32Constants.PRIME32_2;
                        v2 += ((uint*)buffer)[1] * Hashing.XXHash32Constants.PRIME32_2;
                        v3 += ((uint*)buffer)[2] * Hashing.XXHash32Constants.PRIME32_2;
                        v4 += ((uint*)buffer)[3] * Hashing.XXHash32Constants.PRIME32_2;

                        buffer += 4 * sizeof(uint);

                        v1 = Bits.RotateLeft32(v1, 13);
                        v2 = Bits.RotateLeft32(v2, 13);
                        v3 = Bits.RotateLeft32(v3, 13);
                        v4 = Bits.RotateLeft32(v4, 13);

                        v1 *= Hashing.XXHash32Constants.PRIME32_1;
                        v2 *= Hashing.XXHash32Constants.PRIME32_1;
                        v3 *= Hashing.XXHash32Constants.PRIME32_1;
                        v4 *= Hashing.XXHash32Constants.PRIME32_1;
                    }
                    while (buffer <= limit);

                    h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                }
                else
                {
                    h32 = seed + Hashing.XXHash32Constants.PRIME32_5;
                }

                h32 += (uint)len;

                while (buffer + 4 <= bEnd)
                {
                    h32 += *((uint*)buffer) * Hashing.XXHash32Constants.PRIME32_3;
                    h32 = Bits.RotateLeft32(h32, 17) * Hashing.XXHash32Constants.PRIME32_4;
                    buffer += 4;
                }

                while (buffer < bEnd)
                {
                    h32 += (uint)(*buffer) * Hashing.XXHash32Constants.PRIME32_5;
                    h32 = Bits.RotateLeft32(h32, 11) * Hashing.XXHash32Constants.PRIME32_1;
                    buffer++;
                }

                h32 ^= h32 >> 15;
                h32 *= Hashing.XXHash32Constants.PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= Hashing.XXHash32Constants.PRIME32_3;
                h32 ^= h32 >> 16;

                return h32;
            }
        }
    }
}
