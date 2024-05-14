namespace Corax;

public class SupportedFeatures
{
    public readonly bool PhraseQuery = true;
    public readonly bool StoreOnly = true;

    public SupportedFeatures(bool isPhraseQuerySupported, bool isStoreOnlySupported)
    {
        PhraseQuery = isPhraseQuerySupported;
        StoreOnly = isStoreOnlySupported;
    }
    
    public SupportedFeatures()
    {
        // all is supported
    }
}
