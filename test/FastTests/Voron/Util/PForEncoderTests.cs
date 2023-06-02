using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Linq.Indexing;
using Sparrow.Server;
using Sparrow.Threading;
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
    public void CanRespectBufferBoundary()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var encoder = new FastPForEncoder(bsc);
        using var stream = typeof(PForEncoderTests).Assembly
            .GetManifestResourceStream(typeof(PForEncoderTests).Namespace + ".WriteTooMuchToBuffer.txt");
        using var reader = new StreamReader(stream);
        var list = new List<long>();
        while (true)
        {
            string line = reader.ReadLine();
            if (line == null)
                break;
            list.Add(long.Parse(line));
        }

        fixed(byte* buffer = new byte[8128])
        fixed (long* l = list.ToArray())
        {
            var size = encoder.Encode(l, list.Count);
            (int count, int sizeUsed) = encoder.Write(buffer, 8128);
            Assert.True(sizeUsed <= 8128);
            (int count2, int sizeUsed2) = encoder.Write(buffer, 8128);
            Assert.True(sizeUsed2 <= 8128);
            Assert.Equal(list.Count, count + count2);
        }

    }
    
}
