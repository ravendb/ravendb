using System;
using Corax.Utils;

namespace Corax.Indexing;

internal struct TermInEntryModification : IEquatable<TermInEntryModification>, IComparable<TermInEntryModification>
{
    public long EntryId;
    public int TermsPerEntryIndex; 
    public short Frequency;
    
    /// <summary>
    /// Determines if a term for an entryId had a numeric value during entry building.
    /// This prevents only textual terms from being incorrectly marked as numeric in the IndexEntry.
    /// </summary>
    public InserterMode InserterMode;

    public override string ToString() => EntryId + ", " + Frequency;

    public TermInEntryModification(long entryId, int termPerEntryIndex, short frequency, InserterMode inserterMode)
    {
        EntryId = entryId;
        TermsPerEntryIndex = termPerEntryIndex;
        Frequency = frequency;
        InserterMode = inserterMode;
    }

    public bool Equals(TermInEntryModification other)
    {
        return EntryId == other.EntryId && EntryIdEncodings.FrequencyQuantization(Frequency) == EntryIdEncodings.FrequencyQuantization(other.Frequency);
    }

    public int CompareTo(TermInEntryModification other)
    {
        var entryIdComparison = EntryId.CompareTo(other.EntryId);
        if (entryIdComparison != 0)
            return entryIdComparison;
        return EntryIdEncodings.FrequencyQuantization(Frequency).CompareTo(EntryIdEncodings.FrequencyQuantization(other.Frequency));
    }
}
