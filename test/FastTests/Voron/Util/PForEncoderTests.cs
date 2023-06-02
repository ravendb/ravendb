using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Linq.Indexing;
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
    
    [Fact]
    public void CanRespectBufferBoundaryForBuffer()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var encoder = new FastPForEncoder(bsc);
        var array = ReadNumbers("SmallBufferSizeMisleading");

        fixed(byte* buffer = new byte[Container.MaxSizeInsideContainerPage])
        fixed (long* l = array)
        {
            var size = encoder.Encode(l, array.Length);
            Assert.True(size < Container.MaxSizeInsideContainerPage);
            (int count, int sizeUsed) = encoder.Write(buffer, Container.MaxSizeInsideContainerPage);
            Assert.True(sizeUsed <= Container.MaxSizeInsideContainerPage);
            Assert.Equal(array.Length, count);
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
