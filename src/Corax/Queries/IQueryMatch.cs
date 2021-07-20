using System;

namespace Corax.Queries
{
    public static class QueryMatch
    {
        public const long Invalid = -1;
        public const long Start = 0;
    }

    public interface IQueryMatch
    {
        long Count { get; }

        // Guarantees: The output of Fill will be sorted and deduplicated for the call.
        //             Different calls to Fill may return identical values are not guaranteed to be sorted between calls.
        //             0 return means no more matches. 
        int Fill(Span<long> matches);


        // Guarantees: AndWith accepts sorted and returns sorted.
        //             May optimize for continued sorted.
        //             0 return means no more matches from the provided span, and may need to go to the next batch
        int AndWith(Span<long> prevMatches);
    }
}
