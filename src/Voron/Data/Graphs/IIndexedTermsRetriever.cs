using System;

namespace Voron.Data.Graphs;

public interface IIndexedTermsRetriever
{
    public bool GetNextTerm(out ReadOnlySpan<byte> term);
    
    public ConvertTo Type { get; }
}

public enum ConvertTo
{
    String,
    Base64
}
