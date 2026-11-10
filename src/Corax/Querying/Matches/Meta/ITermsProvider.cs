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
        /// A provider that knows the length of a term writes it to <paramref name="termLengths"/> at the same index,
        /// unless <paramref name="termLengths"/> is empty; the others leave it untouched.
        /// Returns the number of IDs written to <paramref name="postingListIds"/>.
        /// Returns 0 when no more terms remain.
        /// </summary>
        int FillPostingListIds(Span<long> postingListIds, Span<int> termLengths);

        void Reset();
        QueryInspectionNode Inspect();

        string DebugView => Inspect().ToString();
    }
}
