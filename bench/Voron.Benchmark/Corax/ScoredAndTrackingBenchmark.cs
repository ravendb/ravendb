using System;
using System.Collections.Generic;
using System.IO;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;

namespace Voron.Benchmark.Corax;

// A scored AND remembers which matches it returned. Sweeps the result count against the index size to find where that tracking gets expensive.
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ScoredAndTrackingBenchmark
{
    private const int DataVersion = 1;
    private const int PageSize = 128;
    private static readonly int[] AllDivisors = [100_000, 10_000, 1_000, 256, 100, 32, 10, 4];

    [Params(1_000_000, 4_000_000)]
    public int NumberOfEntries { get; set; }

    // Each side matches 2 * NumberOfEntries / Divisor entries, the AND returns NumberOfEntries / Divisor
    [ParamsSource(nameof(Divisors))]
    public int Divisor { get; set; }

    [Params(BitmapAndFillMode.Auto, BitmapAndFillMode.Force, BitmapAndFillMode.Off)]
    public BitmapAndFillMode Mode { get; set; }

    public static IEnumerable<int> Divisors => AllDivisors;

    private StorageEnvironment _env;
    private IndexFieldsMapping _mapping;
    private readonly long[] _ids = new long[PageSize];

    [GlobalSetup]
    public void GlobalSetup()
    {
        var path = Path.Combine(Path.GetTempPath(), "Voron.Benchmark", $"CoraxScoredAndTracking-v{DataVersion}-{NumberOfEntries}");
        var reuseIndex = File.Exists(path + ".completed");
        if (reuseIndex == false && Directory.Exists(path))
            Directory.Delete(path, recursive: true);

        _env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(path));
        var builder = IndexFieldsMappingBuilder.CreateForWriter(false);
        for (var level = 0; level < AllDivisors.Length; level++)
            builder.AddBinding(InnerField(level), $"A{level}").AddBinding(OuterField(level), $"B{level}");
        _mapping = builder.Build();

        if (reuseIndex)
            return;

        const int batchSize = 100_000;
        for (var batchStart = 0; batchStart < NumberOfEntries; batchStart += batchSize)
        {
            using var writer = new IndexWriter(_env, _mapping, SupportedFeatures.All);
            for (var i = batchStart; i < Math.Min(batchStart + batchSize, NumberOfEntries); i++)
            {
                using var entry = writer.Index($"items/{i}");
                for (var level = 0; level < AllDivisors.Length; level++)
                {
                    var position = i % AllDivisors[level];
                    if (position is 0 or 1)
                        entry.Write(InnerField(level), "hit"u8);
                    if (position is 0 or 2)
                        entry.Write(OuterField(level), "hit"u8);
                }

                entry.EndWriting();
            }

            writer.Commit();
        }

        File.WriteAllText(path + ".completed", string.Empty);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _mapping.Dispose();
        _env.Dispose();
    }

    // from index 'Items' where A = 'hit' and B = 'hit' order by score()
    [Benchmark]
    public int ScoredAnd()
    {
        using var searcher = new IndexSearcher(_env, _mapping) { BitmapAndFillMode = Mode };
        var level = Array.IndexOf(AllDivisors, Divisor);
        var and = searcher.And(searcher.TermQuery(Field(InnerField(level)), "hit"), searcher.TermQuery(Field(OuterField(level)), "hit"));
        var sorted = searcher.OrderBy(and, new OrderMetadata(true, MatchCompareFieldType.Score), NullsSortMode.NullsSmallest, PageSize);
        return sorted.Fill(_ids);
    }

    private FieldMetadata Field(int fieldId) => _mapping.GetByFieldId(fieldId).Metadata.ChangeScoringMode(true);

    private static int InnerField(int level) => 2 * level;

    private static int OuterField(int level) => 2 * level + 1;
}
