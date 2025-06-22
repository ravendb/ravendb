using System;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Server.Documents.AI;

public interface IChatCompletionClient : IDisposable
{
    public static readonly Regex GoDurationRegex = new(
        @"(?<value>\d+(?:\.\d+)?)(?<unit>ns|us|µs|ms|s|m|h)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant
    );

    public static readonly DocumentConventions DefaultConventions = new() { UseHttpCompression = false };

    Task<(string Result, AiUsage Usage)> CompleteAsync(string prompt, string context, CancellationToken token);
    Task<BlittableJsonReaderObject> GetResponseContentAsync(JsonOperationContext context, HttpResponseMessage response, CancellationToken token);
}

public interface IChatCompletionClientForTesting
{
    public TestingStuff ForTestingPurposesOnly();

    public sealed class TestingStuff
    {
        internal TestingStuff()
        {
        }

        internal Action<AsyncBlittableJsonTextWriter> ModifyPayload;

        internal Action<string> SimulateFailure;
    }
}
