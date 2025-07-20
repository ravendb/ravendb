using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Sparrow.Json;

namespace Raven.Server.Documents.AI;

public interface IChatCompletionClient : IDisposable
{
    public static readonly Regex GoDurationRegex = new(
        @"(?<value>\d+(?:\.\d+)?)(?<unit>ns|us|µs|ms|s|m|h)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant
    );
    Task<(string Result, AiUsage Usage)> CompleteAsync(string systemPrompt, string userPrompt, string schema, List<GenAiAttachment> contextOutputAttachments,
        CancellationToken token);
    
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

        internal Func<string, Task> SimulateFailureAsync;
    }
}
