using System;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Scoring of a term with more documents than Bm25Relevance keeps in memory (104,857), so the posting list is read again during Score.
/// </summary>
[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
public class LargeTermScoreBenchmark
{
    private const int NumberOfEntries = 400_000;
    private const int StatusField = 1;

    [Params("WholeTerm", "RangeUnderAnd", "HeadUnderAnd", "SpreadUnderAnd")]
    public string Scenario { get; set; }

    private StorageEnvironment _env;
    private IndexFieldsMapping _mapping;
    private long[] _matches;
    private float[] _scores;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests());
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(0, "Id").AddBinding(StatusField, "Status").Build();
        using (var writer = new IndexWriter(_env, _mapping, SupportedFeatures.All))
        {
            for (var i = 0; i < NumberOfEntries; i++)
            {
                using var builder = writer.Index($"entries/{i}");
                builder.Write(0, Encoding.UTF8.GetBytes($"entries/{i}"));
                if (i % 5 == 0)
                {
                    builder.Write(StatusField, "inactive"u8);
                }
                else
                {
                    for (var occurrence = 0; occurrence <= i % 4; occurrence++)
                        builder.Write(StatusField, "active"u8);
                }

                builder.EndWriting();
            }

            writer.Commit();
        }

        var termIds = TermIds();
        _matches = Scenario switch
        {
            // where Status = 'active' order by score(): the term scores its own results
            "WholeTerm" => termIds,
            // where Status = 'active' and Price between X and Y order by score(): the And results are a narrow id range
            "RangeUnderAnd" => termIds[150_000..160_000],
            "HeadUnderAnd" => termIds[..10_000],
            // where Status = 'active' and Tag = 'a' order by score(): the And results are spread over the whole term
            "SpreadUnderAnd" => termIds.Where((_, i) => i % 32 == 0).ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(Scenario), Scenario, null)
        };

        _scores = new float[_matches.Length];
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _mapping.Dispose();
        _env.Dispose();
    }

    [Benchmark]
    public float Score()
    {
        using var searcher = new IndexSearcher(_env, _mapping);
        var term = searcher.TermQuery(Field(), "active");
        term.Score(_matches, _scores, 1f);
        return _scores[0];
    }

    private long[] TermIds()
    {
        using var searcher = new IndexSearcher(_env, _mapping);
        var term = searcher.TermQuery(Field(), "active");
        var ids = new long[NumberOfEntries];
        var read = 0;
        while (term.Fill(ids.AsSpan(read)) is var count and > 0)
            read += count;

        return ids[..read];
    }

    private FieldMetadata Field() => _mapping.GetByFieldId(StatusField).Metadata.ChangeScoringMode(true);
}
