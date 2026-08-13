using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Issues;

public class RavenDB_26721 : RavenTestBase
{
    public RavenDB_26721(ITestOutputHelper output) : base(output)
    {
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Body { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Doc>
    {
        public Index()
        {
            Map = docs => from d in docs select new { d.Body };
            Index(x => x.Body, FieldIndexing.Search);
            Store(x => x.Body, FieldStorage.Yes);
        }
    }

    private class Projection
    {
        public string Body { get; set; }
    }

    [RavenMultiplatformFact(RavenTestCategory.Querying | RavenTestCategory.Encryption | RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
    public async Task Streaming_Projection_Query_On_Encrypted_Db_Aborted_MidStream_Does_Not_Throw()
    {
        // Repro for RavenDB-26721: during a streaming projection query over an encrypted database, the index read
        // transaction could be disposed on one thread while a VoronStream read was still in flight on another thread
        // (Lucene caches VoronStream instances per-thread via SegmentReader, and the streaming loop can resume its
        // async continuation on a different thread). The transaction's OnDispose handler nulled LuceneVoronStream.Llt,
        // and the in-flight read then dereferenced the now-null Llt in VoronStream.UpdateLastPageIfNeeded -> NRE,
        // which surfaced to the client as a confusing "Invalid JSON" error.
        //
        // It only manifests on encrypted databases because the encrypted read path calls Llt.GetPageWithoutCache at
        // every page boundary (decrypting each page) - that is ~100-1000x slower than the memory-mapped non-encrypted
        // path, widening the race window enough for the concurrent dispose to land inside an in-flight read.

        var enc = await Encryption.EncryptedServerAsync();
        var cert = enc.Certificates.ServerCertificateForCommunication.Value;

        using var store = GetDocumentStore(new Options
        {
            AdminCertificate = cert,
            ClientCertificate = cert,
            ModifyDatabaseName = _ => enc.DatabaseName,
            ModifyDatabaseRecord = r => r.Encrypted = true,
            Path = NewDataPath()
        });

        await new Index().ExecuteAsync(store);

        const int docCount = 50_000;
        var filler = new string('x', 300);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < docCount; i++)
                await bulk.StoreAsync(new Doc { Body = "common " + filler + " " + i });
        }

        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(20));

        Exception failure = null;
        var deadline = DateTime.UtcNow.AddMinutes(3);

        var workers = Enumerable.Range(0, 32).Select(_ => Task.Run(async () =>
        {
            while (DateTime.UtcNow < deadline && Volatile.Read(ref failure) == null)
            {
                try
                {
                    using var session = store.OpenAsyncSession();
                    var q = session.Advanced.AsyncDocumentQuery<Doc, Index>()
                        .Search(x => x.Body, "common")
                        .SelectFields<Projection>();

                    await using var stream = await session.Advanced.StreamAsync(q);

                    int c = 0;
                    while (await stream.MoveNextAsync())
                    {
                        if (++c >= 2000)
                            break; // read many, then abort (dispose) -> abort the request mid-stream
                    }
                }
                catch (Exception e)
                {
                    var msg = e.GetType().Name + ": " + e.Message;
                    if (msg.Contains("Invalid JSON") || msg.Contains("NullReference"))
                        Interlocked.CompareExchange(ref failure, e, null);

                    // any other exception (e.g. an expected cancellation/abort of the stream) is ignored on purpose
                }
            }
        })).ToArray();

        await Task.WhenAll(workers);

        Assert.True(failure == null,
            "Streaming an encrypted projection query and aborting mid-stream must not surface a NullReferenceException " +
            "(seen by the client as 'Invalid JSON'). Got: " + failure);
    }
}
