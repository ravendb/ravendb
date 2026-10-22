using System;

namespace Corax.Querying.Matches.Meta
{
    public interface ITermProvider
    {
        bool IsFillSupported { get; }

        int Fill(Span<long> containers);
        
        void Reset();
        bool Next(out long termId, out double termRatioToWholeCollection);
        QueryInspectionNode Inspect();

        string DebugView => Inspect().ToString();
    }    
}
