using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron;

public class RavenDB_26927(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public async Task LargeDocumentsShouldNotBeReportedAsGaps()
    {
        using var store = GetDocumentStore();
        
        await using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < 50; i++)
                await bulk.StoreAsync(new Item { Payload = GetRandomString(1024 * 1024, 2026) }, "items/" + i);
        }

        using (var commands = store.Commands())
        {
            var database = Uri.EscapeDataString(store.Database);
            await commands.ExecuteJsonAsync($"/admin/storage/manual-flush?name={database}&type=Documents", HttpMethod.Post, payload: null);

            string report = null;
            await AssertWaitForValueAsync(async () =>
            {
                var command = new GetPagesReportCommand($"name={database}&type=Documents&output=Text");
                await commands.ExecuteAsync(command);
                report = command.Result;
                return report.Contains("/LargeValue") || report.Contains("Gaps");
            }, true, timeout: 30_000);

            Assert.Contains("/LargeValue", report);
            Assert.DoesNotContain("Gaps", report);
        }
    }

    private static string GetRandomString(int length, int seed)
    {
        const string Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        var random = new Random(seed);
        return new string(Enumerable.Repeat(Chars, length).Select(s => s[random.Next(s.Length)]).ToArray());
    }
    
    private sealed class GetPagesReportCommand : RavenCommand<string>
    {
        private readonly string _query;

        public GetPagesReportCommand(string query)
        {
            _query = query;
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/storage/environment/debug-only/pages?{_query}";

            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8);
            Result = reader.ReadToEnd();
        }
    }

    private class Item
    {
        public string Payload { get; set; }
    }
}
