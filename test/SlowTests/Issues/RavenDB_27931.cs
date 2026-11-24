using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27931 : RavenTestBase
{
    public RavenDB_27931(ITestOutputHelper output) : base(output)
    {
    }

    // ~50MB response: far more than Kestrel, the socket buffers and the client can buffer,
    // so the server has to wait for the client to read before it can finish writing
    private const int NumberOfDocs = 200;
    private static readonly string Payload = new('a', 256 * 1024);

    private const int OperationTimeoutInSec = 3;

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.ClientApi)]
    public async Task LoadByIdMustNotHoldTheReadTransactionLongerThanTheOperationTimeoutWhileAClientIsNotReadingTheResponse()
    {
        var url = new StringBuilder("docs?");
        for (var i = 0; i < NumberOfDocs; i++)
            url.Append(i == 0 ? "" : "&").Append("id=items%2F").Append(i);

        await AssertReadTransactionIsReleasedWhileTheClientIsNotReadingAsync(url.ToString(), callerName: "GetDocumentsByIdImplAsync");
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.ClientApi)]
    public async Task GetDocumentsMustNotHoldTheReadTransactionLongerThanTheOperationTimeoutWhileAClientIsNotReadingTheResponse()
    {
        await AssertReadTransactionIsReleasedWhileTheClientIsNotReadingAsync($"docs?startsWith=items%2F&pageSize={NumberOfDocs}", callerName: "GetDocumentsImplAsync");
    }

    private async Task AssertReadTransactionIsReleasedWhileTheClientIsNotReadingAsync(string relativeUrl, string callerName)
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Databases.OperationTimeout)] = OperationTimeoutInSec.ToString()
        });

        await StoreItemsAsync(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using var client = new HttpClient();
        using var response = await client.GetAsync($"{store.Urls[0]}/databases/{store.Database}/{relativeUrl}", HttpCompletionOption.ResponseHeadersRead);
        Assert.True(response.IsSuccessStatusCode, response.StatusCode.ToString());

        // the response is being sent and the client does not read it: the read transaction must be released once the operation times out,
        // well before the 120 seconds of inactivity after which the response stream would have given up
        var openTransactions = await WaitForValueAsync(() =>
                database.DocumentsStorage.Environment.ActiveTransactions.AllTransactionsInstances
                    .Count(tx => tx.CallerName == callerName && tx.IsDisposed == false),
            expectedVal: 0, timeout: 30_000);

        Assert.Equal(0, openTransactions);

        // the response was cut off
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var stream = await response.Content.ReadAsStreamAsync();
            await stream.CopyToAsync(Stream.Null);
        });
    }

    private static async Task StoreItemsAsync(IDocumentStore store)
    {
        await using var bulk = store.BulkInsert();
        for (var i = 0; i < NumberOfDocs; i++)
            await bulk.StoreAsync(new Item { Payload = Payload }, $"items/{i}");
    }

    private class Item
    {
        public string Payload { get; set; }
    }
}
