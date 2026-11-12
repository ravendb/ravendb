using System;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;

namespace Voron.Benchmark.Corax;

/// <summary>
/// where Tags = 'a' or Tags = 'b' order by score(): scoring and ranking of all matches.
/// </summary>
[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
public class ScoreSortBenchmark
{
    private const int TagsField = 1;
    private const int EntriesPerCommit = 1_000_000;

    [Params(100_000, 1_000_000, 10_000_000)]
    public int NumberOfEntries { get; set; }

    // -1 is a query without limit
    [Params(-1, 10)]
    public int Take { get; set; }

    private StorageEnvironment _env;
    private IndexFieldsMapping _mapping;
    private long[] _results;

    [GlobalSetup]
    public void GlobalSetup()
    {
        // Every case runs in its own process, so the index is kept on disk and built only once per size.
        var path = Path.Combine(@"D:\temp", nameof(ScoreSortBenchmark), NumberOfEntries.ToString());
        var builtMarker = path + ".built";
        var isBuilt = File.Exists(builtMarker);
        if (isBuilt == false && Directory.Exists(path))
            Directory.Delete(path, recursive: true);

        _env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(path));
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(0, "Id").AddBinding(TagsField, "Tags").Build();
        if (isBuilt == false)
        {
            IndexEntries();
            File.WriteAllText(builtMarker, string.Empty);
        }

        _results = new long[NumberOfEntries];
    }

    private void IndexEntries()
    {
        for (var start = 0; start < NumberOfEntries; start += EntriesPerCommit)
        {
            using var writer = new IndexWriter(_env, _mapping, SupportedFeatures.All);
            for (var i = start; i < Math.Min(start + EntriesPerCommit, NumberOfEntries); i++)
            {
                using var builder = writer.Index($"entries/{i}");
                builder.Write(0, Encoding.UTF8.GetBytes($"entries/{i}"));
                if (i % 2 == 0)
                {
                    for (var occurrence = 0; occurrence <= i / 2 % 16; occurrence++)
                        builder.Write(TagsField, "a"u8);
                }

                if (i % 3 != 0)
                {
                    for (var occurrence = 0; occurrence <= i / 3 % 16; occurrence++)
                        builder.Write(TagsField, "b"u8);
                }

                builder.EndWriting();
            }

            writer.Commit();
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _mapping.Dispose();
        _env.Dispose();
    }

    [Benchmark]
    public int OrderByScore()
    {
        using var searcher = new IndexSearcher(_env, _mapping);
        var field = _mapping.GetByFieldId(TagsField).Metadata.ChangeScoringMode(true);
        var query = searcher.Or(searcher.TermQuery(field, "a"), searcher.TermQuery(field, "b"));
        var sorted = searcher.OrderBy(query, new OrderMetadata(true, MatchCompareFieldType.Score), NullsSortMode.NullsSmallest, Take);

        var read = 0;
        while (sorted.Fill(_results.AsSpan(read)) is var count and > 0)
            read += count;

        return read;
    }
}
