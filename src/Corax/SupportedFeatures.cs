namespace Corax;

public class SupportedFeatures(bool isPhraseQuerySupported, bool isStoreOnlySupported, bool isPaginationBasedOnEntryIdSupported, bool isNumericTreesWithoutFrequenciesSupported)
{
    public static readonly SupportedFeatures All = new (true, true, true, true);

    public readonly bool PhraseQuery = isPhraseQuerySupported;
    public readonly bool StoreOnly = isStoreOnlySupported;
    public readonly bool PaginationBasedOnEntryId = isPaginationBasedOnEntryIdSupported;
    public readonly bool NumericTreesWithoutFrequencies = isNumericTreesWithoutFrequenciesSupported;
}
