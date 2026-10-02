using System.Diagnostics;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Quill.Contracts;
using Sparrow.Json;

namespace Raven.Quill.Agents;

public static class AgentQueryTester
{
    public const int MaxResults = 100;

    public static async Task<TestQueryResponse> RunAsync(
        IDocumentStore store, string database, TestQueryRequest request, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var query = session.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(request.Query);
        foreach (var (name, value) in request.Parameters ?? [])
            query.AddParameter(name, ToParameterValue(value));

        var stopwatch = Stopwatch.StartNew();
        var results = new List<JsonElement>();
        var isTruncated = false;
        await using var stream = await session.Advanced.StreamAsync(query, out var stats, ct);
        while (await stream.MoveNextAsync())
        {
            if (results.Count == MaxResults)
            {
                isTruncated = true;
                break;
            }

            using var document = JsonDocument.Parse(stream.Current.Document.ToString());
            results.Add(document.RootElement.Clone());
        }

        return new TestQueryResponse(results.ToArray(), stats.TotalResults, isTruncated, stopwatch.ElapsedMilliseconds);
    }

    private static object? ToParameterValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Number:
                return element.TryGetInt64(out var integer) ? integer : element.GetDouble();
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            case JsonValueKind.Array:
                return element.EnumerateArray().Select(ToParameterValue).ToList();
            case JsonValueKind.Object:
                return element.EnumerateObject().ToDictionary(p => p.Name, p => ToParameterValue(p.Value));
            default:
                return null;
        }
    }
}
