using FastTests;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Util.PFor;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public unsafe class RavenDB_27624(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanRoundTripABlockWithThirtyTwoBitExceptions()
    {
        // The values share their low 10 bits, so the encoder works on the values shifted by 10. Then 40% of the deltas are 2^31 and the
        // rest are 2^32 or 0, which have a zero low half. The cheapest encoding is a width of 0 with about 100 exceptions of 32 bits.
        var values = new long[256];
        long value = 1L << 20;
        for (int i = 0; i < values.Length; i++)
        {
            value += i % 5 < 2 ? 1L << 41 : 1L << 42;
            values[i] = value;
        }

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var encoder = new FastPForEncoder(bsc);
        using var decoder = new FastPForDecoder(bsc);
        var decoded = new long[values.Length + 256];

        fixed (byte* buffer = new byte[8128])
        fixed (long* input = values)
        fixed (long* result = decoded)
        {
            encoder.Encode(input, values.Length);
            (int count, int sizeUsed) = encoder.Write(buffer, 8128);
            Assert.Equal(values.Length, count);

            decoder.Init(buffer, sizeUsed);
            Assert.Equal(values.Length, decoder.Read(result, decoded.Length));
        }

        Assert.Equal(values, decoded[..values.Length]);
    }
}
