using Corax.Mappings;
using Corax.Utils;

namespace Corax.Querying.Planning;

public sealed class SortMetadataTemplate
{
    public bool NoSort { get; init; }

    /// <summary>True when no ORDER BY but <c>HasBoost</c>.</summary>
    public bool ImplicitScore { get; init; }

    public bool HasVectorSearch { get; init; }

    public OrderMetadata[] Prebuilt { get; init; }

    /// <summary>Per-query patches for slots whose <see cref="OrderMetadata"/> must be re-resolved each query.</summary>
    public SortSlotPatch[] Patches { get; init; }
}

public struct SortSlotPatch
{
    public SortSlotPatchKind Kind;
    public string FieldName;

    // Index into the live (per-query) ORDER BY array, to read parameters values
    public int OrderByIndex;
}

public enum SortSlotPatchKind : byte
{
    /// <summary>Slot is fully baked — runtime returns the prefab entry verbatim.</summary>
    None = 0,

    /// <summary>Field-backed sort slot holds transaction-bound slices, so it must be re-resolved every query.</summary>
    FieldRuntimeResolve,

    /// <summary>Random ordering with no Arguments — need a new seed each query.</summary>
    RandomFreshSeed,

    /// <summary>Random ordering with a seed (<c>random(123)</c> or <c>random($p)</c>) read from the live ORDER BY arguments.</summary>
    RandomSeeded,

    /// <summary>Distance ordering whose center point/units (literal or parameter) are read from the live ORDER BY arguments.</summary>
    DistanceRuntime,
}
