using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_27606_ScoredAndModes(ITestOutputHelper output) : StorageTest(output)
{
    private const int NumberOfEntries = 20_000;

    public static IEnumerable<object[]> Shapes() => QueryShapes.Keys.Select(x => new object[] { x });

    private static readonly Dictionary<string, Func<IndexSearcher, IQueryMatch>> QueryShapes = new()
    {
        ["(A=a1 and B=b2) or C=c2"] = s => s.Or(s.And(T(s, "A", "a1"), T(s, "B", "b2")), T(s, "C", "c2")),
        ["(A=a1 and B=b1) or C=c1"] = s => s.Or(s.And(T(s, "A", "a1"), T(s, "B", "b1")), T(s, "C", "c1")),
        ["(A=a2 and B=b2) or (C=c2 and D=d2)"] = s => s.Or(s.And(T(s, "A", "a2"), T(s, "B", "b2")), s.And(T(s, "C", "c2"), T(s, "D", "d2"))),
        ["((A=a1 and B=b2) or C=c2) and D=d1"] = s => s.And(s.Or(s.And(T(s, "A", "a1"), T(s, "B", "b2")), T(s, "C", "c2")), T(s, "D", "d1")),
        ["B=b2 and ((A=a1 and D=d1) or C=c2)"] = s => s.And(T(s, "B", "b2"), s.Or(s.And(T(s, "A", "a1"), T(s, "D", "d1")), T(s, "C", "c2"))),
        ["B=b2 and ((A=a1 and not D=d1) or C=c2)"] = s => s.And(T(s, "B", "b2"), s.Or(s.AndNot(T(s, "A", "a1"), T(s, "D", "d1")), T(s, "C", "c2"))),
        ["((A=a1 and B=b1) and C=c2) or D=d2"] = s => s.Or(s.And(s.And(T(s, "A", "a1"), T(s, "B", "b1")), T(s, "C", "c2")), T(s, "D", "d2")),
        ["((A=a1 and B=b2) and not D=d1) or C=c2"] = s => s.Or(s.AndNot(s.And(T(s, "A", "a1"), T(s, "B", "b2")), T(s, "D", "d1")), T(s, "C", "c2")),
        ["(A=a1 and startsWith(P, 'p1')) or C=c2"] = s => s.Or(s.And(T(s, "A", "a1"), s.StartWithQuery(F(s, "P"), "p1")), T(s, "C", "c2")),
        ["(A=a2 and B=b3) or C=c1"] = s => s.Or(s.And(T(s, "A", "a2"), T(s, "B", "b3")), T(s, "C", "c1")),
        ["A=a1 and B=b2"] = s => s.And(T(s, "A", "a1"), T(s, "B", "b2")),
    };

    [RavenTheory(RavenTestCategory.Corax)]
    [MemberData(nameof(Shapes))]
    public void BitmapAndMergePathsGiveSameScores(string shape)
    {
        using var mapping = IndexEntries();

        var reference = Run(mapping, shape, BitmapAndFillMode.Force);
        foreach (var mode in new[] { BitmapAndFillMode.Off, BitmapAndFillMode.Auto })
        {
            var actual = Run(mapping, shape, mode);
            Assert.Equal(reference.Ids, actual.Ids);

            // Summation order may differ between the paths, so only the last float bits are allowed to differ
            var maxRelativeDifference = reference.Scores.Zip(actual.Scores, (x, y) => Math.Abs(x - y) / Math.Max(1f, Math.Abs(x))).DefaultIfEmpty(0).Max();
            var bitExact = reference.Scores.Zip(actual.Scores).Count(x => x.First == x.Second);
            Output.WriteLine($"{shape} | {mode} | results: {actual.Ids.Length} | bit-exact: {bitExact} | max relative difference: {maxRelativeDifference:G3}");

            Assert.True(maxRelativeDifference < 1e-5f, $"{mode}: max relative difference {maxRelativeDifference}");
        }
    }

    private (long[] Ids, float[] Scores) Run(IndexFieldsMapping mapping, string shape, BitmapAndFillMode mode)
    {
        using var searcher = new IndexSearcher(Env, mapping) { BitmapAndFillMode = mode };
        var match = QueryShapes[shape](searcher);

        var ids = new List<long>();
        var buffer = new long[256];
        while (match.Fill(buffer) is var read and > 0)
            ids.AddRange(buffer.AsSpan(0, read).ToArray());

        var sorted = ids.Distinct().Order().ToArray();
        var scores = Enumerable.Repeat(Bm25Relevance.InitialScoreValue, sorted.Length).ToArray();
        match.Score(sorted, scores, 1f);
        return (sorted, scores);
    }

    private static FieldMetadata F(IndexSearcher searcher, string field) => searcher.FieldMetadataBuilder(field, FieldIds[field], hasBoost: true);

    private static IQueryMatch T(IndexSearcher searcher, string field, string value) => searcher.TermQuery(F(searcher, field), value);

    private static readonly Dictionary<string, int> FieldIds = new() { ["Id"] = 0, ["A"] = 1, ["B"] = 2, ["C"] = 3, ["D"] = 4, ["P"] = 5 };

    private IndexFieldsMapping IndexEntries()
    {
        var builder = IndexFieldsMappingBuilder.CreateForWriter(false);
        foreach (var (name, id) in FieldIds)
            builder.AddBinding(id, name);
        var mapping = builder.Build();

        using var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (var i = 0; i < NumberOfEntries; i++)
        {
            using var entry = writer.Index($"e/{i}");
            entry.Write(0, Encoding.UTF8.GetBytes($"e/{i}"));

            // Repeated values give the terms different frequencies, so BM25 scores differ per document
            if (i % 2 == 0)
                Repeat(entry, 1, "a1", 1 + i % 3);
            if (i % 50 == 0)
                Repeat(entry, 1, "a2", 1);
            if (i % 3 == 0)
                Repeat(entry, 2, "b1", 1 + i % 4);
            if (i % 97 == 0)
                Repeat(entry, 2, "b2", 1 + i % 2);
            if (i % 50 == 25)
                Repeat(entry, 2, "b3", 1);
            if (i % 5 == 0)
                Repeat(entry, 3, "c1", 1 + i % 5);
            if (i % 7 == 1)
                Repeat(entry, 3, "c2", 1);
            if (i % 11 == 0)
                Repeat(entry, 4, "d1", 1);
            if (i % 13 == 2)
                Repeat(entry, 4, "d2", 1 + i % 3);
            Repeat(entry, 5, $"p{i % 40}", 1);

            entry.EndWriting();
        }

        writer.Commit();
        return mapping;
    }

    private static void Repeat(IndexWriter.IndexEntryBuilder entry, int field, string value, int times)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        for (var i = 0; i < times; i++)
            entry.Write(field, bytes);
    }
}
