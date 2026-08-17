using System;
using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server;
using Xunit;

namespace DocumentsTxCache.Benchmark;

[MemoryDiagnoser]
public class DocumentsTxCacheBench
{
    // ConsoleTestOutputHelper marks the RavenTestBase instance as running outside of xUnit,
    // which lifts the requirement to tag compression usage with RavenTestCategory.Compression
    private static readonly Tests.Infrastructure.ConsoleTestOutputHelper TestOutputHelper = new Tests.Infrastructure.ConsoleTestOutputHelper();

    private ActualTests _tests;

    [Params(300)]
    public int NumberOfCollections { get; set; }

    [Params(StorageMode.Plain, StorageMode.Compressed, StorageMode.Encrypted)]
    public StorageMode Mode { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tests = new ActualTests(TestOutputHelper);
        _tests.Initialize(NumberOfCollections, Mode);
    }

    [Benchmark]
    public void ModifyDocumentInLoop()
    {
        if (_tests == null)
        {
            throw new InvalidOperationException("'_tests' cannot be null");
        }

        _tests.ModifyDocumentInLoop();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _tests?.DisposeAsync().GetAwaiter().GetResult();
        _tests = null;
    }
}

public class ActualTests : RavenTestBase
{
    private const int DocumentsPerCollection = 100;
    private const int DocumentSizeInBytes = 100 * 1024;
    private const int ModificationsPerInvocation = 5;

    private IDocumentStore _store;
    private string _docIdToModify;
    private int _counter;

    public ActualTests(ITestOutputHelper output) : base(output)
    {
    }

    public string ServerUrl => Server.WebUrl;

    public void Initialize(int numberOfCollections, StorageMode mode)
    {
        var compress = mode == StorageMode.Compressed;
        var encrypt = mode == StorageMode.Encrypted;

        string databaseName = null;
        if (encrypt)
        {
            // AllowEncryptedDatabasesOverHttp is an internal field compile-gated behind
            // ALLOW_ENCRYPTED_OVER_HTTP (off in Release); flip it on the running server via reflection
            // so we can use an encrypted database without setting up an HTTPS/certificate server.
            typeof(RavenServer)
                .GetField("AllowEncryptedDatabasesOverHttp", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(Server, true);

            AsyncHelpers.RunSync(() => Server.ServerStore.EnsureNotPassiveAsync());

            databaseName = "DocumentsTxCacheEncrypted";
            var key = new byte[32];
            RandomNumberGenerator.Fill(key);
            Server.ServerStore.PutSecretKey(Convert.ToBase64String(key), databaseName, overwrite: true);
        }

        _store = GetDocumentStore(new Options
        {
            // persist to disk (not the in-memory pager) so the storage file is really memory-mapped
            RunInMemory = false,
            Encrypted = encrypt,
            ModifyDatabaseName = databaseName == null ? null : _ => databaseName,
            ModifyDatabaseRecord = record => record.DocumentsCompression =
                new DocumentsCompressionConfiguration(compressRevisions: false, compressAllCollections: compress)
        });

        // each document is a distinct ~256KB complex object (many scalar fields + a long list of
        // line items with varied values). the last document by etag per collection is the one
        // ComputeTransactionCache_BeforeCommit reads on every commit.
        var seed = 0;
        using (var bulkInsert = _store.BulkInsert())
        {
            for (var c = 0; c < numberOfCollections; c++)
            {
                var collection = $"Benchmark{c:D3}";

                for (var i = 0; i < DocumentsPerCollection; i++)
                {
                    bulkInsert.Store(CreateComplexDocument(DocumentSizeInBytes, seed++), $"{collection}/{i}", new MetadataAsDictionary
                    {
                        [Constants.Documents.Metadata.Collection] = collection
                    });
                }
            }
        }

        // report the on-disk size of an untouched collection so the compression ratio can be computed
        var details = _store.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());
        if (details.Collections.TryGetValue("Benchmark001", out var stats) && stats.CountOfDocuments > 0)
        {
            Console.WriteLine($"[SIZES] mode={mode} docs={stats.CountOfDocuments} " +
                              $"onDiskTotalBytes={stats.DocumentsSize.SizeInBytes} " +
                              $"onDiskPerDocBytes={stats.DocumentsSize.SizeInBytes / stats.CountOfDocuments}");
        }

        _docIdToModify = "Benchmark000/0";
    }

    public void ModifyDocumentInLoop()
    {
        // the transaction cache is seeded once (during setup); every modify here is steady-state:
        // a full collection scan in the baseline vs the incremental path in the optimized build
        for (var iteration = 0; iteration < ModificationsPerInvocation; iteration++)
        {
            using (var session = _store.OpenSession())
            {
                var doc = session.Load<Doc>(_docIdToModify);
                doc.Version = _counter++;
                session.SaveChanges();
            }
        }
    }

    private static readonly string[] Words =
    {
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
        "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
        "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray"
    };

    private static Doc CreateComplexDocument(int targetSizeInBytes, int seed)
    {
        var doc = new Doc
        {
            Name = $"Entity {seed} {Words[seed % Words.Length]}",
            Description = $"Auto-generated complex benchmark document #{seed} exercising the documents transaction cache.",
            Category = Words[(seed / 3) % Words.Length],
            Version = 1,
            Amount = 1000 + seed * 1.5,
            IsActive = (seed & 1) == 0,
            Tags = new List<string>(),
            Items = new List<LineItem>()
        };

        for (var t = 0; t < 6; t++)
            doc.Tags.Add(Words[(seed + t) % Words.Length]);

        // grow the line-item list until the estimated serialized size reaches the target;
        // values vary per item and per document, so the payload compresses realistically (not like repeated text)
        var approxBytes = 400;
        var i = 0;
        while (approxBytes < targetSizeInBytes)
        {
            var item = new LineItem
            {
                Sku = $"SKU-{seed:D5}-{i:D6}",
                ProductName = $"{Words[i % Words.Length]} {Words[(i / 5) % Words.Length]} model {i}",
                Quantity = (i * 7 + seed) % 500 + 1,
                UnitPrice = ((i * 13 + seed) % 100000) * 0.01,
                Discount = (i % 25) * 0.4,
                Notes = $"Item {i} of doc {seed}: {Words[(i + seed) % Words.Length]} {Words[(i * 3 + seed) % Words.Length]} {Words[(i * 7) % Words.Length]}."
            };
            doc.Items.Add(item);
            approxBytes += 110 + item.Sku.Length + item.ProductName.Length + item.Notes.Length;
            i++;
        }

        return doc;
    }

    public override ValueTask DisposeAsync()
    {
        _store?.Dispose();
        _store = null;

        return base.DisposeAsync();
    }
}

public class Doc
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; }
    public int Version { get; set; }
    public double Amount { get; set; }
    public bool IsActive { get; set; }
    public List<string> Tags { get; set; }
    public List<LineItem> Items { get; set; }
}

public class LineItem
{
    public string Sku { get; set; }
    public string ProductName { get; set; }
    public int Quantity { get; set; }
    public double UnitPrice { get; set; }
    public double Discount { get; set; }
    public string Notes { get; set; }
}

public enum StorageMode
{
    Plain,
    Compressed,
    Encrypted
}
