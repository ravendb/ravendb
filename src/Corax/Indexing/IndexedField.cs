using System.Collections.Generic;
using Corax.Analyzers;
using Corax.Mappings;
using Sparrow.Collections;
using Voron;

namespace Corax.Indexing;

internal sealed class IndexedField
{
    public struct SpatialEntry
    {
        public List<(double, double)> Locations;
        public int TermsPerEntryIndex;
    }

    public Dictionary<long, SpatialEntry> Spatial;
    public readonly FastList<EntriesModifications> Storage;
    public readonly Dictionary<Slice, int> Textual;
    public readonly Dictionary<long, int> Longs;
    public readonly Dictionary<double, int> Doubles;
    public Dictionary<Slice, int> Suggestions;
    public readonly Analyzer Analyzer;
    public readonly string NameForStatistics;
    public readonly Slice Name;
    public readonly Slice NameLong;
    public readonly Slice NameDouble;
    public readonly Slice NameTotalLengthOfTerms;
    public readonly int Id;
    public readonly FieldIndexingMode FieldIndexingMode;
    public readonly bool HasSuggestions;
    public readonly bool ShouldStore;
    public bool HasMultipleTermsPerField;
    public long FieldRootPage;

    public override string ToString()
    {
        return Name.ToString() + " Id: " + Id;
    }

    public IndexedField(IndexFieldBinding binding) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble,
        binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore, binding.FieldNameForStatistics)
    {
    }

    public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, string nameForStatistics = null)
    {
        Name = name;
        NameLong = nameLong;
        NameDouble = nameDouble;
        NameTotalLengthOfTerms = nameTotalLengthOfTerms;
        Id = id;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        ShouldStore = shouldStore;
        Storage = new FastList<EntriesModifications>();
        Textual = new Dictionary<Slice, int>(SliceComparer.Instance);
        Longs = new Dictionary<long, int>();
        Doubles = new Dictionary<double, int>();
        FieldIndexingMode = fieldIndexingMode;
        NameForStatistics = nameForStatistics ?? $"Field_{Name}";
    }

    public void Clear()
    {
        Suggestions?.Clear();
        Doubles?.Clear();
        Spatial?.Clear();
        Longs?.Clear();
        Textual?.Clear();
    }
}
