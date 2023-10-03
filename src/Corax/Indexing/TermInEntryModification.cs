using System;
using Corax.Utils;

namespace Corax.Indexing;

internal struct TermInEntryModification : IEquatable<TermInEntryModification>
{
    public long EntryId;
    public int TermsPerEntryIndex; 
    public short Frequency;

    public override string ToString() => EntryId + ", " + Frequency;

    public bool Equals(TermInEntryModification other)
    {
        return EntryId == other.EntryId && EntryIdEncodings.FrequencyQuantization(Frequency) == EntryIdEncodings.FrequencyQuantization(other.Frequency);
    }
}
