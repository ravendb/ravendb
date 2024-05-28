namespace Corax;

public class SupportedFeatures(bool isPhraseQuerySupported, bool isStoreOnlySupported)
{
    public static readonly SupportedFeatures All = new (true, true);
    
    public readonly bool PhraseQuery = isPhraseQuerySupported;
    public readonly bool StoreOnly = isStoreOnlySupported;
}
