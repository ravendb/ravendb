using System;
using BenchmarkDotNet.Attributes;
using Corax.Utils;

namespace Voron.Benchmark.Corax;

public class CoraxEntryIdEncodings
{
    [Params(16, 256, 1024, 1 << 13, 1 << 14)]
    public int BufferSize { get; set; }

    private long[] _idsWithEncodings;
    
    
    [GlobalSetup]
    public void GlobalSetup()
    {
        _idsWithEncodings = new long[BufferSize]; // executed once per each N value
        var random = new Random();
        for (int i = 0; i < BufferSize; ++i)
        {
            _idsWithEncodings[i] = random.NextInt64(31_111, 99_999) << 10;
        }
    }
    
    [Benchmark]
    public Span<long> DiscardWithSimd()
    {
        EntryIdEncodings.DecodeAndDiscardFrequencySimd(_idsWithEncodings, BufferSize);
        return _idsWithEncodings;
    }
    
    [Benchmark]
    public Span<long> ClassicDiscard()
    {
        EntryIdEncodings.DecodeAndDiscardFrequency(_idsWithEncodings, BufferSize);
        return _idsWithEncodings;
    }
}
