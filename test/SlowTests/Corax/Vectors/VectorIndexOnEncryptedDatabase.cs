using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax.Vectors;

// The HNSW node cache is rebuilt from the post-commit hook, which reads through the committed
// write tx; on encrypted storage those pages were already encrypted in place for the journal.
// Encrypted databases therefore run vector indexes cache-less: indexing must complete without
// errors and queries must resolve from disk.
public class VectorIndexOnEncryptedDatabase(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Encryption, RavenArchitecture.AllX64, LicenseRequired = true)]
    public async Task VectorIndexOnEncryptedDatabaseIndexesAndQueries()
    {
        var databaseName = Encryption.SetupEncryptedDatabase(out var certificates, out _);

        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.AdminCertificate = certificates.ServerCertificate.Value;
        options.ClientCertificate = certificates.ServerCertificate.Value;
        options.ModifyDatabaseName = _ => databaseName;
        options.ModifyDatabaseRecord += r => r.Encrypted = true;

        using var store = GetDocumentStore(options);

        var random = new Random(42);
        var first = new float[] { 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f };
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Document { Embeddings = first });
            for (int i = 0; i < 200; i++)
            {
                var v = new float[8];
                for (int j = 0; j < v.Length; j++)
                    v[j] = (float)(random.NextDouble() * 2 - 1);
                await session.StoreAsync(new Document { Embeddings = v });
            }

            await session.SaveChangesAsync();
        }

        await new NumericalVectorIndex().ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);
        Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));

        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<Document, NumericalVectorIndex>()
                .VectorSearch(x => x.WithField(f => f.Vector), f => f.ByEmbedding(first), 0.99f)
                .ToListAsync();

            Assert.NotEmpty(results);
        }
    }

    private class NumericalVectorIndex : AbstractIndexCreationTask<Document>
    {
        public NumericalVectorIndex()
        {
            Map = docs => from doc in docs
                select new { Vector = CreateVector(doc.Embeddings) };

            VectorIndexes.Add(x => x.Vector, new VectorOptions { SourceEmbeddingType = VectorEmbeddingType.Single });
        }
    }

    private class Document
    {
        public string Id { get; set; }
        public float[] Embeddings { get; set; }
        public object Vector { get; set; }
    }
}
