using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace FastTests.Sparrow
{
    public class BitVectorsTests
    {
        public static IEnumerable<object[]> VectorSize
        {
            get
            {
                return new[]
                {
                    new object[] {1},
                    new object[] {4},
                    new object[] {7},
                    new object[] {8},
                    new object[] {9},
                    new object[] {16},
                    new object[] {17},
                    new object[] {65},
                    new object[] {90},
                    new object[] {244},
                    new object[] {513},
                };
            }
        }

        [Fact]
        public void Constants()
        {
            Assert.Equal(64, BitVector.BitsPerWord);
            Assert.Equal((uint)Math.Log(BitVector.BitsPerWord, 2), BitVector.Log2BitsPerWord);
        }

        [Fact]
        public void Operations_Bits()
        {
            Assert.Equal(0x0000000000000001UL, BitVector.BitInWord(63));
            Assert.Equal(0x0000000000000002UL, BitVector.BitInWord(62));
            Assert.Equal(0x0000000000000004UL, BitVector.BitInWord(61));
            Assert.Equal(0x0000000000000008UL, BitVector.BitInWord(60));
            Assert.Equal(0x8000000000000000UL, BitVector.BitInWord(0));

            for ( int i = 0; i < BitVector.BitsPerWord; i++ )
                Assert.Equal(BitVector.BitInWord(i), BitVector.BitInWord(i + BitVector.BitsPerWord));

            Assert.Equal(0U, BitVector.WordForBit(0));
            Assert.Equal(0U, BitVector.WordForBit(1));
            Assert.Equal(0U, BitVector.WordForBit(63));
            Assert.Equal(1U, BitVector.WordForBit(64));
            Assert.Equal(1U, BitVector.WordForBit(127));
            Assert.Equal(2U, BitVector.WordForBit(128));

            Assert.Equal(0, BitVector.NumberOfWordsForBits(0));
            Assert.Equal(1, BitVector.NumberOfWordsForBits(1));
            Assert.Equal(2, BitVector.NumberOfWordsForBits(128));
            Assert.Equal(3, BitVector.NumberOfWordsForBits(129));
        }

        [Theory]
        [MemberData("VectorSize")]
        public void Construction(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);

            Assert.Equal(vectorSize, vector.Count);
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);

            vector = new BitVector(vectorSize);

            Assert.Equal(vectorSize, vector.Count);
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);
        }

        [Fact]
        public void Construction_Explicit()
        {
            var vector = BitVector.Of(0xFFFFFFFF, 0x00000000);
            Assert.Equal(64, vector.Count);

            for (int i = 0; i < 32; i++)
            {
                Assert.True(vector[i]);
                Assert.False(vector[32 + i]);
            }

            var vector2 = BitVector.Of(0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00);
            Assert.Equal(0, vector.CompareTo(vector2));

            vector = BitVector.Of(0xFFFFFFFF, 0x01010101);

            vector2 = BitVector.Of(0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x01, 0x01);
            Assert.Equal(vector2.Count, vector.LongestCommonPrefixLength(vector2));

            vector2 = BitVector.Of(0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x01);
            Assert.Equal(vector2.Count, vector.LongestCommonPrefixLength(vector2));

            vector2 = BitVector.Of(0xFF, 0xFF, 0xFF, 0xFF, 0x01);
            Assert.Equal(vector2.Count, vector.LongestCommonPrefixLength(vector2));

            vector2 = BitVector.Of(0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01);
            Assert.Equal(80, vector2.Count);
        }

        [Fact]
        public void Construction_PrefixFree()
        {
            var vector = BitVector.Of(false, "1");
            var prefixFreeVector = BitVector.Of(true, "1");

            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));

            vector = BitVector.Of(false, "10");
            prefixFreeVector = BitVector.Of(true, "10");

            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));

            vector = BitVector.Of(false, "13");
            prefixFreeVector = BitVector.Of(true, "13");

            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));

            vector = BitVector.Of(false, 94949L, 1231L);
            prefixFreeVector = BitVector.Of(true, 94949L, 1231L);

            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));

            vector = BitVector.Of(false, 94949u, 1231u);
            prefixFreeVector = BitVector.Of(true, 94949u, 1231u);
            
            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));

            vector = BitVector.Of(false, 94949u, 1231u, 1231u);
            prefixFreeVector = BitVector.Of(true, 94949u, 1231u, 1231u);

            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));

            vector = BitVector.Of(false, (byte)123, (byte)55, (byte)55, (byte)55);
            prefixFreeVector = BitVector.Of(true, (byte)123, (byte)55, (byte)55, (byte)55);

            Assert.Equal(vector.Count + 2 * BitVector.BitsPerByte, prefixFreeVector.Count);
            Assert.True(vector.IsProperPrefix(prefixFreeVector));
        }


        [Theory]
        [MemberData("VectorSize")]
        public void SetBy_Index(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector[i]);
                vector[i] = true;

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i == j));

                Assert.True(vector[i]);
                vector[i] = false;

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector[j]);
            }
        }

        [Theory]
        [MemberData("VectorSize")]
        public void SetBy_Method(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector.Get(i));
                vector.Set(i, true);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector.Get(j) == (i == j));

                Assert.True(vector[i]);
                vector.Set(i, false);

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector.Get(j));
            }
        }

        [Theory]
        [MemberData("VectorSize")]
        public void Flip_Bit(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector[i]);
                vector.Flip(i);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i == j));

                Assert.True(vector[i]);
                vector.Flip(i);

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector[j]);
            }
        }

        [Fact]
        public void Flip_BitsExplicit()
        {
            var v1 = BitVector.Of(0xFFFFFFFF, 0x00000000);
            var v2 = BitVector.Of(0xFFFFFFFF, 0x000000FF);

            v1.Flip(64 - 2, 64);

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000);
            v2 = BitVector.Of(0xFFFFFF00, 0xFF000000);

            v1.Flip(32 - 2, 32 + 2);

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00001100);
            v2 = BitVector.Of(0xFFFFFFFF, 0x000000FF, 0xFF000000, 0x00001100);

            v1.Flip(64 - 8, 64 + 8);

            Assert.Equal(0, v1.CompareTo(v2));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00001100);
            v2 = BitVector.Of(0xFFFFFFFF, 0x000000FF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFF000000, 0x00001100);

            v1.Flip(64 - 8, 64 + 64 + 8);

            Assert.Equal(0, v1.CompareTo(v2));
        }

        [Theory]
        [MemberData("VectorSize")]
        public void Flip_Bits(int vectorSize)
        {
            Random rnd = new Random(1000);

            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                int k = rnd.Next(vectorSize - i);

                Assert.False(vector[i]);
                vector.Flip(i, i + k);

                for (int j = 0; j < vectorSize; j++)
                {
                    if (j >= i && j < i + k)
                        Assert.True(vector[j]);
                    else if (j == i)
                        Assert.True(vector[j]);
                    else
                        Assert.False(vector[j]);
                }

                vector.Flip(i, i + k);

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector[j]);
            }
        }

        [Theory]
        [MemberData("VectorSize")]
        public void Fill_Clear(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            vector.Fill(true);
            for (int i = 0; i < vectorSize; i++)
                Assert.True(vector[i]);

            vector.Clear();
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);
        }

        [Theory]
        [MemberData("VectorSize")]
        public void Operations_IdentityEqualSizes(int vectorSize)
        {
            Random rnd = new Random(1000);

            ulong[] data = new ulong[vectorSize];
            for (int i = 0; i < data.Length; i++)
                data[i] = (ulong)(rnd.Next() << 32 | rnd.Next());

            var v1 = BitVector.Of(data);
            var v2 = BitVector.OfLength(v1.Count);
            v1.CopyTo(v2);

            var rAnd = BitVector.And(v1, v2);
            var rOr = BitVector.Or(v1, v2);
            var rXor = BitVector.Xor(v1, v2);

            BitVector[] array = { rAnd, rOr, rXor };
            foreach (var r in array)
            {
                Assert.Equal(v1.Count, r.Count);
                Assert.Equal(v2.Count, r.Count);
            }

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.Equal(rAnd[i], v1[i] && v2[i]);
                Assert.Equal(rOr[i], v1[i] || v2[i]);
                Assert.Equal(rXor[i], v1[i] ^ v2[i]);
            }
        }

        private static readonly Random generator = new Random(1000);

        [Theory]
        [MemberData("VectorSize")]
        public void Operations_IdentityDifferentSizes(int vectorSize)
        {
            ulong[] data = GenerateRandomArray(vectorSize);
            ulong[] dataPlus = GenerateRandomArray(vectorSize + BitVector.BitsPerWord);

            var v1 = BitVector.Of(data);
            var v2 = BitVector.Of(dataPlus);

            var rAnd = BitVector.And(v1, v2);
            var rOr = BitVector.Or(v1, v2);
            var rXor = BitVector.Xor(v1, v2);

            BitVector[] array = { rAnd, rOr, rXor };
            foreach (var r in array)
            {
                Assert.Equal(v1.Count, r.Count);
                Assert.True(v2.Count > r.Count);
            }

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.Equal(rAnd[i], v1[i] && v2[i]);
                Assert.Equal(rOr[i], v1[i] || v2[i]);
                Assert.Equal(rXor[i], v1[i] ^ v2[i]);
            }
        }

        [Theory]
        [MemberData("VectorSize")]
        public void Operations_IdentityDifferentSizesInverted(int vectorSize)
        {
            ulong[] data = GenerateRandomArray(vectorSize);
            ulong[] dataPlus = GenerateRandomArray(vectorSize + BitVector.BitsPerWord);

            var v1 = BitVector.Of(data);
            var v2 = BitVector.Of(dataPlus);

            var rAnd = BitVector.And(v2, v1);
            var rOr = BitVector.Or(v2, v1);
            var rXor = BitVector.Xor(v2, v1);

            BitVector[] array = { rAnd, rOr, rXor };
            foreach (var r in array)
            {
                Assert.Equal(v1.Count, r.Count);
                Assert.True(v2.Count > r.Count);
            }

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.Equal(rAnd[i], v1[i] && v2[i]);
                Assert.Equal(rOr[i], v1[i] || v2[i]);
                Assert.Equal(rXor[i], v1[i] ^ v2[i]);
            }
        }

        [Fact]
        public void Operations_Compare()
        {
            var v1 = BitVector.Parse("1");
            var v2 = BitVector.Parse("0");

            Assert.Equal(1, v1.CompareTo(v2));

            v1 = BitVector.Parse("10");
            v2 = BitVector.Parse("01");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Parse("10000000");
            v2 = BitVector.Parse("01000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Parse("11000000");
            v2 = BitVector.Parse("01000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Parse("11000000");
            v2 = BitVector.Parse("10000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Parse("11000000111");
            v2 = BitVector.Parse("11000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.Parse("01000000111");
            v2 = BitVector.Parse("01000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.Parse("01000000111");
            v2 = BitVector.Parse("01000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00001100);
            v2 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000100);
        
            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));
        }

        [Fact]
        public void Operations_Compare_Lexicographic()
        {
            var v1 = BitVector.Of("1");
            var v2 = BitVector.Of("0");

            Assert.Equal(1, v1.CompareTo(v2));

            v1 = BitVector.Of("10");
            v2 = BitVector.Of("01");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Of("10000000");
            v2 = BitVector.Of("01000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Of("11000000");
            v2 = BitVector.Of("01000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Of("11000000");
            v2 = BitVector.Of("10000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.Of("11000000111");
            v2 = BitVector.Of("11000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.Of("01000000111");
            v2 = BitVector.Of("01000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.Of("01000000111");
            v2 = BitVector.Of("01000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));
        }

        [Fact]
        public void Operations_LongestCommonPrefix()
        {
            var v1 = BitVector.Parse("1");
            var v2 = BitVector.Parse("0");

            Assert.Equal(0, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("10");
            v2 = BitVector.Parse("11");

            Assert.Equal(1, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("10000000");
            v2 = BitVector.Parse("10000000");

            Assert.Equal(8, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("11000000");
            v2 = BitVector.Parse("11000001");

            Assert.Equal(7, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("11000000111");
            v2 = BitVector.Parse("11000000111");

            Assert.Equal(11, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("01000000111");
            v2 = BitVector.Parse("01000000110");

            Assert.Equal(10, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("");
            v2 = BitVector.Parse("01000000111");

            Assert.Equal(0, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Parse("00000000010001110000000001011000000000000011001100000000001101110000000000000000");
            v2 = BitVector.Parse("00000000011001100000000000110000000000000011010000000000011011110000000000000000");
            
            Assert.Equal(10, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000);
            v2 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000001);

            Assert.Equal(127, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000);
            v2 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000);

            Assert.Equal(128, v1.LongestCommonPrefixLength(v2));
            Assert.Equal(v1.LongestCommonPrefixLength(v2), v2.LongestCommonPrefixLength(v1));
        }

        [Fact]
        public void Operations_GetByte()
        {
            var v1 = BitVector.Of(0x01020304, 0x05060708, 0x090A0B0C, 0x0D0E0F10);
            for( int i = 0; i < v1.Count / BitVector.BitsPerByte; i++ )
                Assert.Equal(i+1, v1.GetByte(i));
        }

        [Fact]
        public void Operations_IsPrefix()
        {
            var v1 = BitVector.Parse("1");
            var v2 = BitVector.Parse("0");

            Assert.False(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));            

            v1 = BitVector.Parse("10");
            v2 = BitVector.Parse("11");

            Assert.False(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));
            Assert.True(v2.IsPrefix(v1, 0));
            Assert.True(v2.IsPrefix(v1, 1));
            Assert.False(v2.IsPrefix(v1, 2));            

            v1 = BitVector.Parse("10000000");
            v2 = BitVector.Parse("10000000");

            Assert.True(v1.IsPrefix(v2));
            Assert.True(v2.IsPrefix(v1));

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.True(v2.IsPrefix(v1, i));
                Assert.True(v1.IsPrefix(v2, i));
            }
                

            v1 = BitVector.Parse("1100000");
            v2 = BitVector.Parse("11000001");

            Assert.True(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));      

            v1 = BitVector.Parse("1100000011");
            v2 = BitVector.Parse("11000000111");

            Assert.True(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));     

            v1 = BitVector.Parse("0100000011");
            v2 = BitVector.Parse("01000000110");

            Assert.True(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.True(v2.IsPrefix(v1, i));
                Assert.True(v1.IsPrefix(v2, i));
            }

            Assert.False(v1.IsPrefix(v2, v2.Count));

            v1 = BitVector.Parse("");
            v2 = BitVector.Parse("01000000111");

            Assert.True(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000);
            v2 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000);

            Assert.True(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));     

            v1 = BitVector.Of(0xFFFFFFFF);
            v2 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000);

            Assert.True(v1.IsPrefix(v2));
            Assert.False(v2.IsPrefix(v1));     
        }

        [Fact]
        public void Operations_SubVector()
        {
            var v1 = BitVector.Parse("1");
            var v2 = v1.SubVector(0, 1);

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v1.SubVector(0, 0).CompareTo(BitVector.Parse(string.Empty)));

            v1 = BitVector.Parse("10");
            Assert.Equal(0, v1.SubVector(0, 1).CompareTo(BitVector.Parse("1")));
            Assert.Equal(0, v1.SubVector(1, 1).CompareTo(BitVector.Parse("0")));

            v1 = BitVector.Parse("10001100");
            Assert.Equal(0, v1.SubVector(0, 5).CompareTo(BitVector.Parse("10001")));
            Assert.Equal(0, v1.SubVector(4, 4).CompareTo(BitVector.Parse("1100")));

            v1 = BitVector.Parse("11001000111");
            Assert.Equal(0, v1.SubVector(0, 9).CompareTo(BitVector.Parse("110010001")));
            Assert.Equal(0, v1.SubVector(1, 9).CompareTo(BitVector.Parse("100100011")));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000100);
            v2 = BitVector.Of(0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00);

            Assert.Equal(0, v1.SubVector(0, 12).CompareTo(BitVector.Of(0xFF, 0xFF, 0xFF)));
            Assert.Equal(0, v1.SubVector(1, 12).CompareTo(BitVector.Parse("11111111111")));
            Assert.Equal(0, v1.SubVector(0, 10).CompareTo(BitVector.Parse("111111111")));
        }

        private static ulong[] GenerateRandomArray(int vectorSize, Random rnd = null)
        {
            if (rnd == null)
                rnd = generator;

            ulong[] data = new ulong[vectorSize];
            for (int i = 0; i < data.Length; i++)
                data[i] = (ulong)(rnd.Next() << 32 | rnd.Next());
            return data;
        }
    }
}
