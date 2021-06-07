using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Compression;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class IntegerEncodingTests : NoDisposalNeeded
    {
        public IntegerEncodingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Compatibility()
        {
            Span<byte> buffer = new byte[16];
            Span<byte> vBuffer = new byte[16];
            fixed (byte* bPtr = buffer, vbPtr = vBuffer)
            {
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

        [Fact]
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

        [Fact]
        public void ReadWriteMany()
        {
            Span<byte> buffer = new byte[VariableSizeEncoding.GetMaximumEncodingLength(16)];
            var rnd = new Random(1337);

            Span<int> values = stackalloc int[16];
            for (int i = 0; i < values.Length; i++)
                values[i] = rnd.Next();

            VariableSizeEncoding.WriteMany(buffer, values);
            
            Span<int> decodedValues = stackalloc int[16];
            VariableSizeEncoding.ReadMany(buffer, 16, decodedValues);

            Assert.Equal(0, values.SequenceCompareTo(decodedValues));
        }

    }
}
