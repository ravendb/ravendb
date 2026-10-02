using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Integration.Benchmark.Corax;

public class ScoresNotConsumedBenchmark : CoraxBenchmarkBase
{
    [Benchmark]
    public long OrderByField() => TotalResults($"from index 'BoostedItems' where Price between 0 and {NumberOfDocuments / 2} order by Name limit 128");

    [Benchmark]
    public long Count() => TotalResults($"from index 'BoostedItems' where Price between 0 and {NumberOfDocuments / 2} limit 0");

    [Benchmark]
    public long StreamingOrder() => TotalResults("from index 'BoostedItems' where startsWith(Name, 'name1') order by Name limit 128");

    private long TotalResults(string query)
    {
        using var session = Store.OpenSession();
        session.Advanced.RawQuery<Item>(query).NoCaching().Statistics(out var statistics).ToList();
        return statistics.TotalResults;
    }
}
