// Expected diagnostic: RVN004 — "AbstractIndexCreationTask subclass is missing a Map assignment"
// Every class inheriting from AbstractIndexCreationTask<T> must assign Map in its constructor.
// Without a Map expression the index has no definition and will fail when deployed.
//
// Fix: add  Map = docs => from d in docs select new { ... };  inside the constructor.
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

// warning RVN004: Class 'RVN004_EmptyIndex' inherits from an index creation task
//                 but does not assign the Map property in its constructor
public class RVN004_EmptyIndex : AbstractIndexCreationTask<RVN004_Order>
{
    public RVN004_EmptyIndex()
    {
        // No Map assignment here!
    }
}

public class RVN004_Order { public string Id { get; set; } = default!; public string Name { get; set; } = default!; }
