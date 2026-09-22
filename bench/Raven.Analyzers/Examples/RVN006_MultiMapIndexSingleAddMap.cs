// Expected diagnostic: RVN006 — "Multi-map index uses only a single AddMap"
// A multi-map index base class is designed for indexing multiple document types.
// If only one AddMap call is present, a regular AbstractIndexCreationTask<T> would
// be simpler and equally expressive.
//
// Fix: either add a second AddMap<T>(...) call, or change the base class to
// AbstractIndexCreationTask<TSource> and assign Map = ... instead.
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Examples;

// warning RVN006: Class 'RVN006_SingleMapIndex' calls AddMap only once.
//                 Consider deriving from AbstractIndexCreationTask<T> instead.
public class RVN006_SingleMapIndex : AbstractMultiMapIndexCreationTask<RVN006_Result>
{
    public RVN006_SingleMapIndex()
    {
        AddMap<RVN006_Company>(companies =>
            from c in companies
            select new RVN006_Result { Name = c.Name });
    }
}

public class RVN006_Result { public string Name { get; set; } = default!; }
public class RVN006_Company { public string Name { get; set; } = default!; }
