using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data.Containers;
using Voron.Util.PFor;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Util;

public unsafe class PForEncoderTests : NoDisposalNeeded
{
    public PForEncoderTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [Theory]
    [InlineData("SmallBufferSizeMisleading")]
    [InlineData("GreaterThan42B")]
    [InlineData("GreaterThan42B-Truncated")] // this ensures the >4.2B is on the last varint block
    [InlineData("SmallBufferMisleading2")]
    public void CanRoundTripSmallContainer(string name)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var encoder = new FastPForEncoder(bsc);
        var array = ReadNumbers(name);

        fixed (byte* buffer = new byte[Container.MaxSizeInsideContainerPage])
        {
            int size;
            fixed (long* l = array)
            {
                size = encoder.Encode(l, array.Length);
                Assert.True(size < Container.MaxSizeInsideContainerPage);
                (int count, int sizeUsed) = encoder.Write(buffer, Container.MaxSizeInsideContainerPage);
                Assert.True(sizeUsed <= Container.MaxSizeInsideContainerPage);
                Assert.Equal(array.Length, count);
            }

            var output = new long[Bits.PowerOf2(array.Length)];
            using var decoder = new FastPForDecoder(bsc);
            decoder.Init(buffer, size);
            fixed (long* o = output)
            {
                int read = decoder.Read(o, output.Length);
                Assert.Equal(read, array.Length);
                Assert.Equal(array, output.Take(read));
            }
        }
    }
    
    [Fact]
    public void CanRespectBufferBoundaryForPage()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var encoder = new FastPForEncoder(bsc);
        var array = ReadNumbers("WriteTooMuchToBuffer");

        fixed(byte* buffer = new byte[8128])
        fixed (long* l = array)
        {
            var size = encoder.Encode(l, array.Length);
            (int count, int sizeUsed) = encoder.Write(buffer, 8128);
            Assert.True(sizeUsed <= 8128);
            (int count2, int sizeUsed2) = encoder.Write(buffer, 8128);
            Assert.True(sizeUsed2 <= 8128);
            Assert.Equal(array.Length, count + count2);
        }
    }

    private static long[] ReadNumbers(string name)
    {
        using var stream = typeof(PForEncoderTests).Assembly
            .GetManifestResourceStream(typeof(PForEncoderTests).Namespace + "."+ name + ".txt");
        using var reader = new StreamReader(stream);
        var list = new List<long>();
        while (true)
        {
            string line = reader.ReadLine();
            if (line == null)
                break;
            list.Add(long.Parse(line));
        }

        return list.ToArray();
    }
}
