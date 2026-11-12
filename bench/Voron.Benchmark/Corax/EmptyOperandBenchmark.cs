using BenchmarkDotNet.Attributes;
using Corax.Querying;
using Corax.Querying.Matches;

namespace Voron.Benchmark.Corax;

public class EmptyOperandBenchmark : ScoringBenchmarkBase
{
    // from index 'Items' where startsWith(Missing, 'x') and startsWith(Name, 'name1')
    [Benchmark]
    public long AndWithEmptyMultiTerm() => Run(s => Count(s.And(Names(s), s.StartWithQuery(s.FieldMetadataBuilder("Missing"), "x"))));

    // from index 'Items' where Tag = 'missing' and not startsWith(Name, 'name1')
    [Benchmark]
    public long AndNotWithEmptyInner() => Run(s => Count(s.AndNot(s.TermQuery(Field(TagField), "missing"), Names(s))));

    private MultiTermMatch Names(IndexSearcher searcher) => searcher.StartWithQuery(Field(NameField), "name1");
}
