using BenchmarkDotNet.Attributes;

namespace Voron.Benchmark.Corax;

public class DocumentBoostBenchmark : ScoringBenchmarkBase
{
    [Params(1, 10)]
    public int BoostEvery { get; set; }

    protected override int DocumentBoostEvery => BoostEvery;

    // from index 'Items' order by score()
    [Benchmark]
    public long AllEntries() => Run(s => FirstPage(SortByScore(s, s.AllEntries())));

    // from index 'Items' where Tag = 'tag0' order by score()
    [Benchmark]
    public long CommonTerm() => Run(s => FirstPage(SortByScore(s, s.TermQuery(Field(TagField, scored: true), "tag0"))));

    // from index 'Items' where Category = 'category0' order by score()
    [Benchmark]
    public long RareTerm() => Run(s => FirstPage(SortByScore(s, s.TermQuery(Field(CategoryField, scored: true), "category0"))));
}
