using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27931 : RavenTestBase
{
    public RavenDB_27931(ITestOutputHelper output) : base(output)
    {
    }

    private const string LoadByIdCallerName = "GetDocumentsByIdImplAsync";

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.ClientApi)]
    public async Task LoadByIdMustNotHoldTheReadTransactionWhileAClientIsNotReadingTheResponse()
    {
        // ~50MB response: far more than Kestrel, the socket buffers and the client can buffer,
        // so the server has to wait for the client to read before it can finish writing
        const int numberOfDocs = 200;
        var payload = new string('a', 256 * 1024);

        using var store = GetDocumentStore();

        await using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < numberOfDocs; i++)
                await bulk.StoreAsync(new Item { Payload = payload }, $"items/{i}");
        }

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        var url = new StringBuilder($"{store.Urls[0]}/databases/{store.Database}/docs?");
        for (var i = 0; i < numberOfDocs; i++)
            url.Append(i == 0 ? "" : "&").Append("id=items%2F").Append(i);

        using var client = new HttpClient();
        using var response = await client.GetAsync(url.ToString(), HttpCompletionOption.ResponseHeadersRead);
        Assert.True(response.IsSuccessStatusCode, response.StatusCode.ToString());

        // the response is being sent and the client does not read it: the read transaction must not stay open until it does
        var openLoadByIdTransactions = await WaitForValueAsync(() =>
                database.DocumentsStorage.Environment.ActiveTransactions.AllTransactionsInstances
                    .Count(tx => tx.CallerName == LoadByIdCallerName && tx.IsDisposed == false),
            expectedVal: 0, timeout: 15_000);

        Assert.Equal(0, openLoadByIdTransactions);

        // the full response is still delivered once the client reads it
        await using var stream = await response.Content.ReadAsStreamAsync();
        using var json = await JsonDocument.ParseAsync(stream);

        var results = json.RootElement.GetProperty("Results");
        Assert.Equal(numberOfDocs, results.GetArrayLength());
        foreach (var result in results.EnumerateArray())
            Assert.Equal(payload, result.GetProperty(nameof(Item.Payload)).GetString());
    }

    [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Encryption)]
    [InlineData(false, 1024)]
    [InlineData(false, TempFileBufferingWriteStream.MemoryThreshold + 1)]
    [InlineData(false, 3 * TempFileBufferingWriteStream.MemoryThreshold + 12345)]
    [InlineData(true, 3 * TempFileBufferingWriteStream.MemoryThreshold + 12345)]
    public async Task TheBufferReplaysEverythingWrittenToIt(bool encrypted, int size)
    {
        var data = new byte[size];
        new Random(27931).NextBytes(data);

        var tempFilePath = Path.Combine(NewDataPath(forceCreateDir: true), "buffer");

        await using var buffer = new TempFileBufferingWriteStream(() => new StreamsTempFile(tempFilePath, encrypted));

        // uneven chunks, through both the sync and async overloads, so writes straddle the memory threshold and the encryption blocks
        var position = 0;
        var chunk = 1;
        while (position < data.Length)
        {
            var count = Math.Min(chunk, data.Length - position);
            if (chunk % 2 == 0)
                await buffer.WriteAsync(data.AsMemory(position, count));
            else
                buffer.Write(data, position, count);

            position += count;
            chunk = chunk * 3 % 100_003 + 1;
        }

        Assert.Equal(size > TempFileBufferingWriteStream.MemoryThreshold, buffer.SpilledToDisk);

        using var destination = new MemoryStream();
        await buffer.DrainToAsync(destination, CancellationToken.None);

        Assert.Equal(data, destination.ToArray());
    }

    private class Item
    {
        public string Payload { get; set; }
    }
}
