using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Utils;

public readonly record struct TermQueryItem(CompactKey Key, long Density, Slice Term, long ContainerId);
