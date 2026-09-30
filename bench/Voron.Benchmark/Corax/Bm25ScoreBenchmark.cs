using System;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Utils;
using Sparrow.Server;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
public class Bm25ScoreBenchmark
{
    private const int NumberOfEntries = 200_000;

    [Params("RootTerm", "TermUnderAnd", "SmallTermUnderAnd", "MediumTermUnderOr", "RareTermUnderOr", "ManyRareTerms")]
    public string Scenario { get; set; }

    private StorageEnvironment _env;
    private IndexFieldsMapping _mapping;
    private IndexSearcher _searcher;
    private ByteStringContext _allocator;
    private Bm25Relevance[] _terms;
    private long[] _matches;
    private float[] _scores;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(0, "Id").Build();
        using (var writer = new IndexWriter(_env, _mapping, SupportedFeatures.All))
        {
            for (var i = 0; i < NumberOfEntries; i++)
            {
                using var builder = writer.Index($"entries/{i}");
                builder.Write(0, Encoding.UTF8.GetBytes($"entries/{i}"));
                builder.EndWriting();
            }

            writer.Commit();
        }

        _searcher = new IndexSearcher(_env, _mapping);
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);

        (_matches, var terms) = Scenario switch
        {
            // where Tag = 'a' order by score(): the term scores its own results
            "RootTerm" => (Ids(100_000, 1), new[] { Ids(100_000, 1) }),
            // where Tag = 'a' and Category = 'b' order by score(): a large term scores the small result of the And
            "TermUnderAnd" => (Ids(1_000, 100), new[] { Ids(100_000, 1) }),
            "SmallTermUnderAnd" => (Ids(100, 10), new[] { Ids(1_000, 1) }),
            // where Tag = 'a' or Category = 'b' order by score(): a term scores the larger result of the Or
            "MediumTermUnderOr" => (Ids(100_000, 1), new[] { Ids(10_000, 10) }),
            "RareTermUnderOr" => (Ids(100_000, 1), new[] { Ids(100, 1_000) }),
            // where startsWith(Name, 'name') order by score(): every term has one document
            "ManyRareTerms" => (Ids(10_000, 1), Ids(10_000, 1).Select(id => new[] { id }).ToArray()),
            _ => throw new ArgumentOutOfRangeException(nameof(Scenario), Scenario, null)
        };

        _terms = terms.Select(Term).ToArray();
        _scores = new float[_matches.Length];
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        foreach (var term in _terms)
            term.Dispose();
        _allocator.Dispose();
        _searcher.Dispose();
        _mapping.Dispose();
        _env.Dispose();
    }

    [Benchmark]
    public float Score()
    {
        foreach (var term in _terms)
            term.Score(_matches, _scores, 1f);

        return _scores[0];
    }

    private Bm25Relevance Term(long[] ids)
    {
        var term = Bm25Relevance.Small(_searcher, ids.Length, _allocator, ids.Length, termRatioToWholeCollection: 1);
        var encoded = ids.Select((id, i) => EntryIdEncodings.Encode(id, (short)(1 + i % 8), TermIdMask.SmallPostingList)).ToArray();
        term.Process(encoded, encoded.Length);
        return term;
    }

    private static long[] Ids(int count, int stride) => Enumerable.Range(0, count).Select(i => (long)i * stride * 4).ToArray();
}
