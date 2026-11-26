// Expected diagnostic: RVN005 — "Multi-map index has no AddMap call in any constructor"
// Every class inheriting from AbstractMultiMapIndexCreationTask must call AddMap<T>(...)
// at least once in its constructor. Without it the index has no definition.
//
// Fix: add AddMap<TSource>(docs => from d in docs select new { ... }); in the constructor.
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

// warning RVN005: Class 'RVN005_BadMultiMapIndex' inherits from a multi-map index
//                 creation task but does not call AddMap in its constructor
public class RVN005_BadMultiMapIndex : AbstractMultiMapIndexCreationTask<RVN005_Result>
{
    public RVN005_BadMultiMapIndex()
    {
        // No AddMap calls here!
    }
}

public class RVN005_Result { public string Name { get; set; } = default!; }
