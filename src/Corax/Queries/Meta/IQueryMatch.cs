using System;
using System.Diagnostics;

namespace Corax.Queries
{
    public static class QueryMatch
    {
        public const long Invalid = -1;
        public const long Start = 0;
    }

    public enum QueryCountConfidence : int
    {
        Low = 0,
        Normal = 1,
        High = 2,
    }

    public static class QueryConfidenceExtensions
    {
        public static QueryCountConfidence Min(this QueryCountConfidence c1, QueryCountConfidence c2)
        {
            if (c1 < c2)
                return c1;
            return c2;
        }

        public static QueryCountConfidence Max(this QueryCountConfidence c1, QueryCountConfidence c2)
        {
            if (c1 > c2)
                return c1;
            return c2;
        }
    }

    public interface IQueryMatch
    {
        long Count { get; }

        // The confidence of the query count.
        //  - High: We know exactly how many items there are.
        //  - Normal: We know roughly that it is in the order of magnitude.
        //  - Low: We know very little about it.
        QueryCountConfidence Confidence { get; }

        bool IsBoosting { get; }

        // Guarantees: The output of Fill will be sorted and deduplicated for the call.
        //             Different calls to Fill may return identical values are not guaranteed to be sorted between calls.
        //             0 return means no more matches. 
        int Fill(Span<long> matches);

        // Guarantees: AndWith accepts sorted and returns sorted.
        //             May optimize for continued sorted.
        //             0 return means no more matches from the provided span, and may need to go to the next batch
        // Requirements: Cannot be called with .Fill() from same instance.
        int AndWith(Span<long> buffer, int matches);

        // Guarantees: The output of this for unscored sequences should be a no-op.
        // Requirements: The upmost call 
        void Score(Span<long> matches, Span<float> scores, float boostFactor);

        QueryInspectionNode Inspect();

        string DebugView => Inspect().ToString();
    }
}
