using System;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CompactTreeTests : StorageTest
{
    public CompactTreeTests(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineDataWithRandomSeed]
    public unsafe void CanEncodeAndDecode(int seed)
    {
        var r = new Random(seed);
        
        var buffer = stackalloc byte[CompactTree.EncodingBufferSize];

        long key = 0;
        long val = 0;
        for (int i = 0; i < 8; i++)
        {
            key = (key << 8) + r.Next(0, 128);
            val = (val << 8) + r.Next(0, 128);
        
            var len = CompactTree.EncodeEntry(key, val, buffer);
            Assert.True(len <= CompactTree.EncodingBufferSize);
            var lenDecoded = CompactTree.DecodeEntry(buffer, out var k, out var v);
        
            Assert.Equal(len, lenDecoded);
            Assert.Equal(key, k);
            Assert.Equal(val, v);
        }
    }
}
