
/// DO NOT MODIFY.
/// All changes should be done through the usage of the Voron.Dictionary.Generator

namespace Voron.Data.CompactTrees;
partial class PersistentDictionary
{
    public const int MaxDictionaryEntries = 1439;
    private const int DefaultDictionaryTableSize = 65440;
    private const int DefaultAllocationSizeForTable =  DefaultDictionaryTableSize + PersistentDictionaryHeader.SizeOf;
}
