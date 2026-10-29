using BenchmarkDotNet.Attributes;

namespace Integration.Benchmark.Corax;

public class FacetQueryBenchmark : CoraxBenchmarkBase
{
    [Benchmark]
    public int TermFacets() => Facets("from index 'Items' where Tag = 'tag0' select facet(Category)");

    [Benchmark]
    public int RangeFacets() => Facets($"from index 'Items' where Tag = 'tag0' select facet(Price < {NumberOfDocuments / 2}, Price >= {NumberOfDocuments / 2})");

    [Benchmark]
    public int TermFacetsOnBoostedIndex() => Facets("from index 'BoostedItems' where Tag = 'tag0' select facet(Category)");

    private int Facets(string query)
    {
        using var session = Store.OpenSession();
        return session.Advanced.RawQuery<Item>(query).NoCaching().ExecuteAggregation().Count;
    }
}
