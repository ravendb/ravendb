using BenchmarkDotNet.Attributes;

namespace Voron.Benchmark.Corax;

public class NegatedClauseScoringBenchmark : ScoringBenchmarkBase
{
    // from index 'Items' where Tag = 'tag0' and not Price between 0 and N/2 order by score()
    [Benchmark]
    public long AndNotRange() => Run(s => FirstPage(SortByScore(s, s.AndNot(s.TermQuery(Field(TagField, scored: true), "tag0"), s.BetweenQuery(Field(PriceField), 0L, (long)NumberOfEntries / 2)))));

    // from index 'Items' where Tag = 'tag0' and not startsWith(Name, 'name1') order by score()
    [Benchmark]
    public long AndNotStartsWith() => Run(s => FirstPage(SortByScore(s, s.AndNot(s.TermQuery(Field(TagField, scored: true), "tag0"), s.StartWithQuery(Field(NameField), "name1")))));

    // from index 'Items' where Tag != 'tag1' order by score()
    [Benchmark]
    public long NotEquals() => Run(s => FirstPage(SortByScore(s, s.AndNot(s.AllEntries(), s.TermQuery(Field(TagField), "tag1")))));
}
