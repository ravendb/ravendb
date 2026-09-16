namespace Corax.Querying.Planning;

public enum ScanValueType : byte
{
    Long,       // reader.CurrentLong vs ctx.ResidualLongParams[i]
    Double,     // reader.CurrentDouble vs ctx.ResidualDoubleParams[i]
    Slice,      // IL-emitted byte-sequence comparison (UTF-8 ≤ 255 bytes)
    SliceLong,  // Same IL as Slice, but value exceeds 255 bytes — compound-index segment
                // can't hold it. Separate TypeSignature slot ensures the cache key
                // discriminates compound-eligible from ineligible string plans.
}