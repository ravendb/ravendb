using System;
using Sparrow.Compression;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class IntegerEncodingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void Compatibility()
        {
            Span<byte> buffer = new byte[16];
            Span<byte> vBuffer = new byte[16];
            fixed (byte* bPtr = buffer, vbPtr = vBuffer)
            {
                for (int i = 0; i < 2 * ushort.MaxValue; i++)
                {
                    var value = i;

                    buffer.Fill(0);
                    vBuffer.Fill(0);

                    int pos = 0;
                    int length = VariableSizeEncoding.Write<int>(buffer, value);

                    var vbPtrCopy = vbPtr;
                    JsonParserState.WriteVariableSizeInt(ref vbPtrCopy, value);
                    Assert.Equal((int)(vbPtrCopy - vbPtr), length);
                    Assert.Equal(0, vBuffer.SequenceCompareTo(buffer));

                    pos = 0;
                    Assert.Equal(value, BlittableJsonReaderBase.ReadVariableSizeInt(bPtr, ref pos));
                    Assert.Equal(value, VariableSizeEncoding.Read<int>(buffer, out int _, 0));
                }


                var rnd = new Random(1337);
                for (int i = 0; i < 100; i++)
                {
                    var value = rnd.Next();

                    buffer.Fill(0);
                    vBuffer.Fill(0);

                    int pos = 0;
                    int length = VariableSizeEncoding.Write<int>(buffer, value);

                    var vbPtrCopy = vbPtr;
                    JsonParserState.WriteVariableSizeInt(ref vbPtrCopy, value);
                    Assert.Equal((int)(vbPtrCopy - vbPtr), length);
                    Assert.Equal(0, vBuffer.SequenceCompareTo(buffer));

                    pos = 0;
                    Assert.Equal(value, BlittableJsonReaderBase.ReadVariableSizeInt(bPtr, ref pos));
                    Assert.Equal(value, VariableSizeEncoding.Read<int>(buffer, out int _, 0));
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ReadWrite()
        {
            Span<byte> buffer = new byte[16];
            var rnd = new Random(1337);
            for (int i = 0; i < 100; i++)
            {
                buffer.Fill(0);

                var value = rnd.Next();
                VariableSizeEncoding.Write(buffer, value);
                Assert.Equal(value, VariableSizeEncoding.Read<int>(buffer, out int _));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ReadWriteMany()
        {
            Span<byte> buffer = new byte[VariableSizeEncoding.MaximumSizeOf<long>() * 16];
            var rnd = new Random(1337);

            Span<int> values = stackalloc int[16];
            for (int i = 0; i < values.Length; i++)
                values[i] = rnd.Next();

            VariableSizeEncoding.WriteMany(buffer, values);
            
            Span<int> decodedValues = stackalloc int[16];
            VariableSizeEncoding.ReadMany(buffer, 16, decodedValues);

            Assert.Equal(0, values.SequenceCompareTo(decodedValues));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(-1)]
        [InlineData(-2)]
        [InlineData(-3)]
        [InlineData(-127)]
        [InlineData(-128)]
        [InlineData(-1000)]
        [InlineData(int.MinValue)]
        public void ZigZag_Int_Roundtrip(int expected)
        {
            Span<byte> buffer = new byte[16];
            ZigZagEncoding.Encode<int>(buffer, expected);
            int decoded = ZigZagEncoding.Decode<int>(buffer);
            Assert.Equal(expected, decoded);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData((short)-1)]
        [InlineData((short)-2)]
        [InlineData((short)-128)]
        [InlineData(short.MinValue)]
        public void ZigZag_Short_Roundtrip(short expected)
        {
            Span<byte> buffer = new byte[16];
            ZigZagEncoding.Encode<short>(buffer, expected);
            short decoded = ZigZagEncoding.Decode<short>(buffer);
            Assert.Equal(expected, decoded);
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData((sbyte)-1)]
        [InlineData((sbyte)-2)]
        [InlineData(sbyte.MinValue)]
        public void ZigZag_SByte_Roundtrip(sbyte expected)
        {
            Span<byte> buffer = new byte[16];
            ZigZagEncoding.Encode<sbyte>(buffer, expected);
            sbyte decoded = ZigZagEncoding.Decode<sbyte>(buffer);
            Assert.Equal(expected, decoded);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ZigZag_Int_PointerOverload_NegativeValues_Roundtrip()
        {
            Span<byte> buffer = new byte[16];
            fixed (byte* ptr = buffer)
            {
                int[] values = { -1, -2, -1000, int.MinValue };
                foreach (var expected in values)
                {
                    buffer.Clear();
                    ZigZagEncoding.Encode<int>(ptr, expected);
                    int decoded = ZigZagEncoding.Decode<int>(ptr, out _);
                    Assert.Equal(expected, decoded);
                }
            }
        }

    }
}
