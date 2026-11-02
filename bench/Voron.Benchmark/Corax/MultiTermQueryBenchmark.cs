using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;

namespace Voron.Benchmark.Corax;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
public class MultiTermQueryBenchmark
{
    private const int DataVersion = 1;
    private const int PageSize = 128;
    private const int NameField = 0, PriceField = 1, CategoryField = 2;
    private const int Categories = 1_000;

    [Params(100_000, 1_000_000)]
    public int NumberOfEntries { get; set; }

    [Params(false, true)]
    public bool Scored { get; set; }

    private StorageEnvironment _env;
    private IndexFieldsMapping _mapping;
    private List<string> _names;
    private readonly long[] _ids = new long[4096];

    [GlobalSetup]
    public void GlobalSetup()
    {
        var path = Path.Combine(Path.GetTempPath(), "Voron.Benchmark", $"CoraxMultiTerm-v{DataVersion}-{NumberOfEntries}");
        var reuseIndex = File.Exists(path + ".completed");
        if (reuseIndex == false && Directory.Exists(path))
            Directory.Delete(path, recursive: true);

        _env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(path));
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(NameField, "Name").AddBinding(PriceField, "Price").AddBinding(CategoryField, "Category")
            .Build();

        if (reuseIndex == false)
        {
            WriteEntries();
            File.WriteAllText(path + ".completed", string.Empty);
        }

        _names = Enumerable.Range(0, 1_000).Select(i => $"name{i * (NumberOfEntries / 1_000)}").ToList();
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
                builder.Write(NameField, Encoding.UTF8.GetBytes($"name{i}"));
                builder.Write(PriceField, Encoding.UTF8.GetBytes(i.ToString()), i, i);
                builder.Write(CategoryField, Encoding.UTF8.GetBytes($"category{i % Categories}"));
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

    // from index 'Items' where startsWith(Name, 'name1') [order by score()]
    [Benchmark]
    public long StartsWith() => Run(s => s.StartWithQuery(Field(NameField), "name1"));

    // from index 'Items' where search(Name, '*ame12*') [order by score()]
    [Benchmark]
    public long Contains() => Run(s => s.ContainsQuery(Field(NameField), "ame12"));

    // from index 'Items' where Name in (1,000 names) [order by score()]
    [Benchmark]
    public long In() => Run(s => s.InQuery(Field(NameField), _names));

    // from index 'Items' where Price between 0 and N/2 [order by score()]
    [Benchmark]
    public long Range() => Run(s => s.BetweenQuery(Field(PriceField), 0L, (long)NumberOfEntries / 2));

    // from index 'Items' where startsWith(Category, 'category1') [order by score()]
    [Benchmark]
    public long StartsWithOnMultiDocumentTerms() => Run(s => s.StartWithQuery(Field(CategoryField), "category1"));

    private long Run(Func<IndexSearcher, MultiTermMatch> query)
    {
        using var searcher = new IndexSearcher(_env, _mapping);
        var match = query(searcher);
        return Scored
            ? FirstPage(searcher.OrderBy(match, new OrderMetadata(true, MatchCompareFieldType.Score), NullsSortMode.NullsSmallest, PageSize))
            : Count(match);
    }

    private FieldMetadata Field(int fieldId) => _mapping.GetByFieldId(fieldId).Metadata.ChangeScoringMode(Scored);

    private long FirstPage(SortingMatch match)
    {
        var read = match.Fill(_ids.AsSpan(0, PageSize));
        var checksum = match.TotalResults;
        foreach (var id in _ids.AsSpan(0, read))
            checksum += id;

        return checksum;
    }

    private long Count<TMatch>(TMatch match) where TMatch : IQueryMatch
    {
        long count = 0;
        while (match.Fill(_ids) is var read and > 0)
            count += read;

        return count;
    }
}
