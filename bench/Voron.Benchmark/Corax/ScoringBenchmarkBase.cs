using System;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;

namespace Voron.Benchmark.Corax;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
public abstract class ScoringBenchmarkBase
{
    private const int DataVersion = 1;
    private const int PageSize = 128;
    protected const int TagField = 0, CategoryField = 1, PriceField = 2, NameField = 3;
    protected const int Categories = 1_000;

    [Params(100_000, 1_000_000)]
    public int NumberOfEntries { get; set; }

    protected virtual int DocumentBoostEvery => 10;

    private StorageEnvironment _env;
    private IndexFieldsMapping _mapping;
    private readonly long[] _ids = new long[4096];

    [GlobalSetup]
    public void GlobalSetup()
    {
        var path = Path.Combine(Path.GetTempPath(), "Voron.Benchmark", $"CoraxScoring-v{DataVersion}-{NumberOfEntries}" + (DocumentBoostEvery == 10 ? "" : $"-boost{DocumentBoostEvery}"));
        var reuseIndex = File.Exists(path + ".completed");
        if (reuseIndex == false && Directory.Exists(path))
            Directory.Delete(path, recursive: true);

        _env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(path));
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(TagField, "Tag").AddBinding(CategoryField, "Category").AddBinding(PriceField, "Price").AddBinding(NameField, "Name")
            .Build();

        if (reuseIndex == false)
        {
            WriteEntries();
            File.WriteAllText(path + ".completed", string.Empty);
        }
    }

    private void WriteEntries()
    {
        const int batchSize = 100_000;
        for (var batchStart = 0; batchStart < NumberOfEntries; batchStart += batchSize)
        {
            using var writer = new IndexWriter(_env, _mapping, SupportedFeatures.All);
            for (var i = batchStart; i < Math.Min(batchStart + batchSize, NumberOfEntries); i++)
            {
                using var builder = writer.Index($"items/{i}");
                builder.Write(TagField, Encoding.UTF8.GetBytes($"tag{i % 10}"));
                builder.Write(CategoryField, Encoding.UTF8.GetBytes($"category{i % Categories}"));
                builder.Write(PriceField, Encoding.UTF8.GetBytes(i.ToString()), i, i);
                builder.Write(NameField, Encoding.UTF8.GetBytes($"name{i}"));
                if (i % DocumentBoostEvery == 0)
                    builder.Boost(2);
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

    protected long Run(Func<IndexSearcher, long> query)
    {
        using var searcher = new IndexSearcher(_env, _mapping);
        return query(searcher);
    }

    protected FieldMetadata Field(int fieldId, bool scored = false) => _mapping.GetByFieldId(fieldId).Metadata.ChangeScoringMode(scored);

    protected static SortingMatch SortByScore<TMatch>(IndexSearcher searcher, TMatch match, int take = PageSize) where TMatch : IQueryMatch =>
        searcher.OrderBy(match, new OrderMetadata(true, MatchCompareFieldType.Score), NullsSortMode.NullsSmallest, take);

    protected long Count(IQueryMatch match)
    {
        long count = 0;
        while (match.Fill(_ids) is var read and > 0)
            count += read;
        return count;
    }

    protected long FirstPage(IQueryMatch match) => match.Fill(_ids.AsSpan(0, PageSize));
}
