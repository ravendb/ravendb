using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Vector;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27700 : RavenTestBase
{
    private const int DocsCount = 3;
    private const int SnapshotLength = 100;
    private const int SnapshotDocsCount = 5;

    private static readonly string[] Texts =
    [
        "Cats sleep for most of the day",
        "The stock market fell sharply this morning",
        "Fresh bread from the bakery around the corner",
        "A rocket launched a weather satellite into orbit",
        "Heavy rain is expected in the mountains tomorrow",
        "The violin concert was sold out in an hour"
    ];

    public RavenDB_27700(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(100)]
    [InlineData(384)]
    [InlineData(768)]
    [InlineData(1536)]
    public void SingleToBinaryWithExplicitDimensionsIndexesDocuments(int dimensions)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            for (var i = 0; i < 3; i++)
                session.Store(new Doc { Embedding = CreateEmbedding(dimensions, i) }, $"docs/{i}");
            session.SaveChanges();
        }

        var index = new VecIndex(dimensions);
        index.Execute(store);
        Indexes.WaitForIndexing(store, allowErrors: true);

        Assert.Equal(3, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName)).EntriesCount, 3));

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Doc>(index.IndexName)
                .VectorSearch(x => x.WithField("Vector"), v => v.ByEmbedding(CreateEmbedding(dimensions, 0)))
                .ToList();
            Assert.Contains(results, x => x.Id == "docs/0");
        }
    }

    public static IEnumerable<object[]> EmbeddingTypeCombinations()
    {
        (VectorEmbeddingType Source, VectorEmbeddingType Destination)[] pairs =
        [
            (VectorEmbeddingType.Single, VectorEmbeddingType.Single),
            (VectorEmbeddingType.Single, VectorEmbeddingType.Int8),
            (VectorEmbeddingType.Single, VectorEmbeddingType.Binary),
            (VectorEmbeddingType.Int8, VectorEmbeddingType.Int8),
            (VectorEmbeddingType.Binary, VectorEmbeddingType.Binary)
        ];

        foreach (var length in new[] { 8, 16, SnapshotLength, 384, 768, 1536 })
        {
            foreach (var (source, destination) in pairs)
            {
                yield return [source, destination, length, null];
                // a Binary source counts Dimensions in bytes, the size of its array
                yield return [source, destination, length, source is VectorEmbeddingType.Binary ? (length + 7) / 8 : length];

                // Single -> Binary with Dimensions as a byte count is the only form that worked before RavenDB-27700
                if (source is VectorEmbeddingType.Single && destination is VectorEmbeddingType.Binary)
                    yield return [source, destination, length, (length + 7) / 8];
            }
        }

        // Text dimensions are set by the embedder, an explicit value is rejected
        foreach (var destination in new[] { VectorEmbeddingType.Single, VectorEmbeddingType.Int8, VectorEmbeddingType.Binary })
            yield return [VectorEmbeddingType.Text, destination, 0, null];
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [MemberData(nameof(EmbeddingTypeCombinations))]
    public void IndexesAndQueriesEveryEmbeddingTypeCombination(VectorEmbeddingType source, VectorEmbeddingType destination, int length, int? dimensions)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.RunInMemory = false;
        using var store = GetDocumentStore(options);
        StoreDocs(store, length, 0, DocsCount);
        var indexName = PutIndex(store, source, destination, length, dimensions);
        Indexes.WaitForIndexing(store, allowErrors: true);
        AssertIndexed(store, indexName, source, length, DocsCount);

        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
        Indexes.WaitForIndexing(store, allowErrors: true);
        AssertIndexed(store, indexName, source, length, DocsCount);

        StoreDocs(store, length, DocsCount, 1);
        Indexes.WaitForIndexing(store, allowErrors: true);
        AssertIndexed(store, indexName, source, length, DocsCount + 1);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.BackupExportImport)]
    public void IndexesCreatedBeforeTheFixKeepWorkingAfterRestore()
    {
        // Taken on v7.2 before RavenDB-27700: SnapshotDocsCount docs and one index per SnapshotCombinations() entry, plus BinaryToBinary_100_100
        var backupPath = NewDataPath(forceCreateDir: true);
        using (var file = File.Create(Path.Combine(backupPath, "RavenDB_27700.ravendb-snapshot")))
        using (var stream = typeof(RavenDB_27700).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_27700.RavenDB_27700.ravendb-snapshot"))
        {
            Assert.NotNull(stream);
            stream.CopyTo(file);
        }

        using var store = GetDocumentStore(new Options { CreateDatabase = false });
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = store.Database });
        Indexes.WaitForIndexing(store, allowErrors: true);

        var combinations = SnapshotCombinations().ToList();
        // the snapshot also holds a Binary source index with Dimensions in bits, which never worked and stays unsupported
        var bitsIndexName = IndexName(VectorEmbeddingType.Binary, VectorEmbeddingType.Binary, SnapshotLength, SnapshotLength);
        Assert.Equal(combinations.Count + 1, store.Maintenance.Send(new GetIndexNamesOperation(0, int.MaxValue)).Length);

        foreach (var (source, destination, length, dimensions) in combinations)
        {
            var indexName = IndexName(source, destination, length, dimensions);
            if (FailedBeforeTheFix(destination, length, dimensions))
            {
                // every map attempt failed, so the index is in the error state until it is reset
                var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexName));
                Assert.Equal(IndexState.Error, stats.State);
                Assert.Equal(0, stats.EntriesCount);
                store.Maintenance.Send(new ResetIndexOperation(indexName));
            }
            else
                AssertIndexed(store, indexName, source, SnapshotLength, SnapshotDocsCount);
        }

        StoreDocs(store, SnapshotLength, SnapshotDocsCount, 1);
        store.Maintenance.Send(new ResetIndexOperation(bitsIndexName));
        Indexes.WaitForIndexing(store, allowErrors: true);

        foreach (var (source, destination, length, dimensions) in combinations)
            AssertIndexed(store, IndexName(source, destination, length, dimensions), source, SnapshotLength, SnapshotDocsCount + 1);

        Assert.Equal(IndexState.Error, store.Maintenance.Send(new GetIndexStatisticsOperation(bitsIndexName)).State);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void SingleToBinaryWithExplicitDimensionsRejectsOtherSizesAndKeepsTheFirstAcceptedOne()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.RunInMemory = false;
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new EmbeddingDoc { Single = CreateEmbedding(50, 0) }, "docs/0"); // 7 bytes, neither 100 bits nor 100 bytes
            session.Store(new EmbeddingDoc { Single = CreateEmbedding(100, 1) }, "docs/1"); // 13 bytes, sets the size
            session.SaveChanges();
        }

        var indexName = PutIndex(store, VectorEmbeddingType.Single, VectorEmbeddingType.Binary, 100, 100);
        Indexes.WaitForIndexing(store, allowErrors: true);

        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));

        using (var session = store.OpenSession())
        {
            session.Store(new EmbeddingDoc { Single = CreateEmbedding(800, 2) }, "docs/2"); // 100 bytes, valid for the definition but not for the size already set
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store, allowErrors: true);

        Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexErrorsOperation([indexName])).Sum(x => x.Errors.Length), 2));
        Assert.Equal(1, store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount);
        var errors = store.Maintenance.Send(new GetIndexErrorsOperation([indexName])).SelectMany(x => x.Errors).ToDictionary(x => x.Document, x => x.Error);
        Assert.Contains("explicitly configured for embeddings with 100 dimensions", errors["docs/0"]);
        Assert.Contains("contains embeddings with different number of dimensions than currently processed embedding", errors["docs/2"]);
    }

    private static IEnumerable<(VectorEmbeddingType Source, VectorEmbeddingType Destination, int Length, int? Dimensions)> SnapshotCombinations() =>
        EmbeddingTypeCombinations()
            .Select(x => ((VectorEmbeddingType)x[0], (VectorEmbeddingType)x[1], (int)x[2], (int?)x[3]))
            .Where(x => x.Item3 is SnapshotLength or 0);

    private static bool FailedBeforeTheFix(VectorEmbeddingType destination, int length, int? dimensions) =>
        destination is VectorEmbeddingType.Binary && dimensions == length;

    private static string IndexName(VectorEmbeddingType source, VectorEmbeddingType destination, int length, int? dimensions) =>
        $"{source}To{destination}_{length}_{dimensions?.ToString() ?? "NoDimensions"}";

    private static string PutIndex(IDocumentStore store, VectorEmbeddingType source, VectorEmbeddingType destination, int length, int? dimensions)
    {
        var indexName = IndexName(source, destination, length, dimensions);
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = indexName,
            Maps = { $"from d in docs.EmbeddingDocs select new {{ Vector = CreateVector(d.{source}) }}" },
            Fields =
            {
                ["Vector"] = new IndexFieldOptions
                {
                    Vector = new VectorOptions { SourceEmbeddingType = source, DestinationEmbeddingType = destination, Dimensions = dimensions }
                }
            }
        }));
        return indexName;
    }

    private static void StoreDocs(IDocumentStore store, int length, int first, int count)
    {
        using var session = store.OpenSession();
        for (var seed = first; seed < first + count; seed++)
        {
            var embedding = CreateEmbedding(length, seed);
            session.Store(new EmbeddingDoc
            {
                Single = embedding,
                Int8 = VectorQuantizer.ToInt8(embedding),
                Binary = VectorQuantizer.ToInt1(embedding),
                Text = Texts[seed]
            }, $"docs/{seed}");
        }

        session.SaveChanges();
    }

    private static void AssertIndexed(IDocumentStore store, string indexName, VectorEmbeddingType source, int length, int count)
    {
        // WaitForIndexing returns after the batch commit, the entries count and errors are written by a later transaction
        Assert.Equal(count, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, count));
        Assert.Empty(store.Maintenance.Send(new GetIndexErrorsOperation([indexName])).SelectMany(x => x.Errors).Select(x => x.Error));

        var seed = count - 1;
        using var session = store.OpenSession();
        var results = session.Advanced.DocumentQuery<EmbeddingDoc>(indexName)
            .VectorSearch(x => x.WithField("Vector"), v => QueryBy(v, source, length, seed))
            .ToList();
        Assert.Contains(results, x => x.Id == $"docs/{seed}");
    }

    private static void QueryBy(IVectorFieldValueFactory factory, VectorEmbeddingType source, int length, int seed)
    {
        switch (source)
        {
            case VectorEmbeddingType.Single:
                factory.ByEmbedding(CreateEmbedding(length, seed));
                break;
            case VectorEmbeddingType.Int8:
                factory.ByEmbedding(VectorQuantizer.ToInt8(CreateEmbedding(length, seed)));
                break;
            case VectorEmbeddingType.Binary:
                factory.ByBase64(Convert.ToBase64String(VectorQuantizer.ToInt1(CreateEmbedding(length, seed))));
                break;
            case VectorEmbeddingType.Text:
                factory.ByText(Texts[seed]);
                break;
        }
    }

    private static float[] CreateEmbedding(int dimensions, int seed) =>
        Enumerable.Range(0, dimensions).Select(i => (float)Math.Sin((i + 1) * (seed + 1))).ToArray();

    private class VecIndex : AbstractIndexCreationTask<Doc>
    {
        public VecIndex(int dimensions)
        {
            Map = docs => from doc in docs
                select new { Vector = CreateVector(doc.Embedding) };

            VectorIndexes.Add(x => x.Vector, new VectorOptions
            {
                SourceEmbeddingType = VectorEmbeddingType.Single,
                DestinationEmbeddingType = VectorEmbeddingType.Binary,
                Dimensions = dimensions
            });
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public float[] Embedding { get; set; }
        public object Vector { get; set; }
    }

    private class EmbeddingDoc
    {
        public string Id { get; set; }
        public float[] Single { get; set; }
        public sbyte[] Int8 { get; set; }
        public byte[] Binary { get; set; }
        public string Text { get; set; }
    }
}
