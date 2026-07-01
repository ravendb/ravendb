namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal enum MergeKind
{
    Fill, // slot 0 ← clause result. First op of an OR chain or first non-negated element of an AND chain
    OrInto, // slot 0 ← slot 0 ∪ clause. Subsequent OR-chain elements
    AndInto, // slot 0 ← slot 0 ∩ clause. Subsequent positive AND-chain elements
    AndNotInto // slot 0 ← slot 0 \ clause. Negated AND-chain elements
}
