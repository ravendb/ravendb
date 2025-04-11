using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Corax.Analyzers;
using Corax.Mappings;
using Sparrow.Collections;
using Sparrow.Server;
using Voron;
using Voron.Util;

namespace Corax.Indexing;

internal sealed class IndexedField
{
    private readonly IndexedField _parent;
    public struct SpatialEntry
    {
        public List<(double, double)> Locations;
        public int TermsPerEntryIndex;
    }

    public Dictionary<long, SpatialEntry> Spatial;
    public readonly FastList<EntriesModifications> Storage;
    public readonly Dictionary<Slice, int> Textual;
    
    /// <summary>
    /// Position matches position from _entryToTerms from IndexWriter which creates relation between entry and field
    /// </summary>
    
    private NativeList<NativeList<int>> _entryToTerms;
    public ref NativeList<NativeList<int>> EntryToTerms => ref _parent == null ? ref _entryToTerms : ref _parent._entryToTerms;
    public readonly Dictionary<long, int> Longs;
    public readonly Dictionary<double, int> Doubles;
    public Dictionary<Slice, int> Suggestions;
    public Analyzer Analyzer;
    public readonly string NameForStatistics;
    public readonly Slice Name;
    public readonly Slice NameLong;
    public readonly Slice NameDouble;
    public readonly Slice NameTotalLengthOfTerms;
    public readonly int Id;
    public readonly FieldIndexingMode FieldIndexingMode;
    public readonly bool ShouldIndex;
    public readonly bool HasSuggestions;
    public readonly bool ShouldStore;
    public readonly SupportedFeatures SupportedFeatures;
    public readonly bool IsVirtual;
    public bool HasMultipleTermsPerField;
    public long FieldRootPage;
    public long TermsVectorFieldRootPage;
    public bool FieldSupportsPhraseQuery => SupportedFeatures.PhraseQuery && FieldIndexingMode is FieldIndexingMode.Search;
    public bool IsCreatedByDelete => _isCreatedByField;
    private bool _isCreatedByField;
    
    public override string ToString()
    {
        return Name.ToString() + " Id: " + Id;
    }

    /// <summary>
    /// This constructor allows rewriting the configuration of the indexed field once it has been created.
    /// This is useful for dynamic field scenarios, where a delete operation can create an indexed field (in blank)
    /// since all terms it contains have already been analyzed (from the index entry). However, when in the same batch
    /// we have new documents, we need to update the analyzer, etc., from the binding sent by the indexing batch.
    /// </summary>
    public IndexedField(IndexedField source, IndexFieldBinding binding)
    {
        _parent = source._parent;
        Spatial = source.Spatial;
        Storage = source.Storage;
        Textual = source.Textual;
        _entryToTerms = source._entryToTerms;
        Longs = source.Longs;
        Doubles = source.Doubles;
        Suggestions = source.Suggestions;
        Analyzer = binding.Analyzer ?? source.Analyzer;
        NameForStatistics = source.NameForStatistics;
        Name = source.Name;
        NameLong = source.NameLong;
        NameDouble = source.NameDouble;
        NameTotalLengthOfTerms = source.NameTotalLengthOfTerms;
        Id = source.Id;
        FieldIndexingMode = binding.FieldIndexingMode;
        ShouldIndex = binding.FieldIndexingMode != FieldIndexingMode.No;
        HasSuggestions = binding.HasSuggestions;
        ShouldStore = binding.ShouldStore;
        SupportedFeatures = source.SupportedFeatures;
        IsVirtual = source.IsVirtual;
        HasMultipleTermsPerField = source.HasMultipleTermsPerField;
        FieldRootPage = source.FieldRootPage;
        TermsVectorFieldRootPage = source.TermsVectorFieldRootPage;
        _isCreatedByField = false;
        AssertIndexedFieldClassHasNotChanged();
    }

    public IndexedField(IndexFieldBinding binding, in SupportedFeatures supportedFeatures) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble,
        binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore, supportedFeatures, binding.FieldNameForStatistics)
    {
    }

    private IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, in SupportedFeatures supportedFeatures, string nameForStatistics, long fieldRootPage, long termsVectorFieldRootPage, FastList<EntriesModifications> storage, Dictionary<Slice, int> textual, Dictionary<long, int> longs, Dictionary<double, int> doubles, IndexedField parent, bool isCreatedByDelete)
    {
        _parent = parent;
        Name = name;
        NameLong = nameLong;
        NameDouble = nameDouble;
        NameTotalLengthOfTerms = nameTotalLengthOfTerms;
        Id = id;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        ShouldStore = shouldStore;
        SupportedFeatures = supportedFeatures;
        FieldRootPage = fieldRootPage;
        TermsVectorFieldRootPage = termsVectorFieldRootPage;
        Storage = storage;
        Textual = textual;
        Longs = longs;
        Doubles = doubles;
        FieldIndexingMode = fieldIndexingMode;
        ShouldIndex = supportedFeatures.StoreOnly == false || fieldIndexingMode != FieldIndexingMode.No;
        NameForStatistics = nameForStatistics ?? $"Field_{Name}";
        IsVirtual = true;
        if (fieldIndexingMode is FieldIndexingMode.Search && _parent.EntryToTerms.IsValid == false)
            EntryToTerms = new();

        _isCreatedByField = isCreatedByDelete;
    }
    
    public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, in SupportedFeatures supportedFeatures, string nameForStatistics = null, long fieldRootPage = Constants.IndexWriter.InvalidPageId, long termsVectorFieldRootPage = Constants.IndexWriter.InvalidPageId, bool isCreatedByDelete = false)
    {
        Name = name;
        NameLong = nameLong;
        NameDouble = nameDouble;
        NameTotalLengthOfTerms = nameTotalLengthOfTerms;
        Id = id;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        ShouldStore = shouldStore;
        SupportedFeatures = supportedFeatures;
        FieldRootPage = fieldRootPage;
        TermsVectorFieldRootPage = termsVectorFieldRootPage;
        Storage = new FastList<EntriesModifications>();
        Textual = new Dictionary<Slice, int>(SliceComparer.Instance);
        Longs = new Dictionary<long, int>();
        Doubles = new Dictionary<double, int>();
        FieldIndexingMode = fieldIndexingMode;
        ShouldIndex = supportedFeatures.StoreOnly == false || fieldIndexingMode != FieldIndexingMode.No;
        NameForStatistics = nameForStatistics ?? $"Field_{Name}";
        _isCreatedByField = isCreatedByDelete;
        
        if (fieldIndexingMode is FieldIndexingMode.Search)
            EntryToTerms = new();
    }

    public IndexedField CreateVirtualIndexedField(IndexFieldBinding dynamicField, bool isCreatedByDelete)
    {
        Analyzer analyzer;
        FieldIndexingMode fieldIndexingMode;
        //backward compatibility
        switch (dynamicField.FieldIndexingMode)
        {
            case FieldIndexingMode.No:
                analyzer = null;
                fieldIndexingMode = FieldIndexingMode.No;
                break;
            default:
                analyzer = Analyzer ?? dynamicField.Analyzer;
                fieldIndexingMode = Analyzer is null ? dynamicField.FieldIndexingMode : FieldIndexingMode;
                break;
        }
        
        return new IndexedField(Constants.IndexWriter.DynamicField, Name, NameLong, NameDouble,
            NameTotalLengthOfTerms, analyzer, fieldIndexingMode, dynamicField.HasSuggestions, dynamicField.ShouldStore,
            SupportedFeatures, dynamicField.FieldNameForStatistics, FieldRootPage, TermsVectorFieldRootPage, Storage, Textual, Longs, Doubles, this, isCreatedByDelete);
    }

    public void Clear()
    {
        Suggestions?.Clear();
        Doubles?.Clear();
        Spatial?.Clear();
        Longs?.Clear();
        Textual?.Clear();
        EntryToTerms = default;
    }

    [Conditional("DEBUG")]
    private void AssertIndexedFieldClassHasNotChanged()
    {
        string[] knownFields =
        [
            nameof(_parent), nameof(Spatial), nameof(Storage), nameof(Textual), nameof(_entryToTerms), nameof(Longs), nameof(Doubles), nameof(Suggestions),
            nameof(Analyzer), nameof(NameForStatistics), nameof(Name), nameof(NameLong), nameof(NameDouble), nameof(NameTotalLengthOfTerms), nameof(Id),
            nameof(FieldIndexingMode), nameof(ShouldIndex), nameof(HasSuggestions), nameof(ShouldStore), nameof(SupportedFeatures), nameof(IsVirtual),
            nameof(HasMultipleTermsPerField), nameof(FieldRootPage), nameof(TermsVectorFieldRootPage), nameof(FieldSupportsPhraseQuery), nameof(IsCreatedByDelete), nameof(_isCreatedByField)
        ];

        var fields = this.GetType().GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);

        var diff = fields.Select(x => x.Name).Except(knownFields).ToArray();

        if (diff.Length != 0)
        {
            throw new InvalidDataException(
                $"IndexedField has changed. Please update the following fields: {string.Join(", ", diff)} in the constructor IndexedField(IndexedField source, IndexFieldBinding binding)");
        }
    }
}
