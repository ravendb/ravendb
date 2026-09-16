using System;

namespace Corax.Querying.Matches.Meta
{
    public interface ITermsProvider
    {
        /// <summary>
        /// Yields raw posting list IDs (with TermIdMask encoding) in batches.
        /// Each returned value encodes the posting list type in its low bits
        /// (Single, SmallPostingList, or PostingList) and can be decoded via
        /// EntryIdEncodings / TermIdMask.
        /// Returns the number of IDs written to <paramref name="postingListIds"/>.
        /// Returns 0 when no more terms remain.
        /// </summary>
        int FillPostingListIds(Span<long> postingListIds);

        void Reset();
        QueryInspectionNode Inspect();

        string DebugView => Inspect().ToString();
    }
}
