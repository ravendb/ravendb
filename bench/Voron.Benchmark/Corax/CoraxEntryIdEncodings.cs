using System;
using BenchmarkDotNet.Attributes;
using Corax.Utils;

namespace Voron.Benchmark.Corax;

[DisassemblyDiagnoser(printSource: true)]
public class CoraxEntryIdEncodings
{
    [Params( 256, 1024, 1 << 13, 1 << 14, 1 << 15, 1 << 16)]
    public int BufferSize { get; set; }

    private long[] _idsWithEncodingsForSimd;
    private long[] _idsWithEncodingsForClassic;
    
    
    [GlobalSetup]
    public void GlobalSetup()
    {
        _idsWithEncodingsForSimd = new long[BufferSize];
        _idsWithEncodingsForClassic = new long[BufferSize];// executed once per each N value
        var random = new Random();
        for (int i = 0; i < BufferSize; ++i)
        {
            _idsWithEncodingsForSimd[i] = random.NextInt64(31_111, 99_999) << 30;
            _idsWithEncodingsForClassic[i] = _idsWithEncodingsForSimd[i];
        }
    }
    
    [Benchmark]
    public Span<long> DiscardWithSimd()
    {
        EntryIdEncodings.DecodeAndDiscardFrequencyAvx2(_idsWithEncodingsForSimd, BufferSize);
        EntryIdEncodings.DecodeAndDiscardFrequencyAvx2(_idsWithEncodingsForSimd, BufferSize);
        EntryIdEncodings.DecodeAndDiscardFrequencyAvx2(_idsWithEncodingsForSimd, BufferSize);

        return _idsWithEncodingsForSimd;
    }
    
    [Benchmark]
    public Span<long> ClassicDiscard()
    {
        EntryIdEncodings.DecodeAndDiscardFrequencyClassic(_idsWithEncodingsForClassic, BufferSize);
        EntryIdEncodings.DecodeAndDiscardFrequencyClassic(_idsWithEncodingsForClassic, BufferSize);
        EntryIdEncodings.DecodeAndDiscardFrequencyClassic(_idsWithEncodingsForClassic, BufferSize);
        return _idsWithEncodingsForClassic;
    }
}
