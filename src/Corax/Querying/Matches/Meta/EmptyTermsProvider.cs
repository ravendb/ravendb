using System;

namespace Corax.Querying.Matches.Meta
{
    /// <summary>Singleton no-op <see cref="ITermsProvider"/> for TreeScan slots where the field
    /// doesn't exist. <see cref="FillPostingListIds"/> returns 0 immediately, so the bitmap op is a
    /// no-op.</summary>
    public sealed class EmptyTermsProvider : ITermsProvider
    {
        public static readonly EmptyTermsProvider Instance = new();

        public int FillPostingListIds(Span<long> postingListIds) => 0;

        public void Reset() { }

        public QueryInspectionNode Inspect() => new("EmptyTermsProvider");
    }
}
