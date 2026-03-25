using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace StressTests.GenAi;

public class ChatCompletionClientStressTests : RavenTestBase
{
    public ChatCompletionClientStressTests(ITestOutputHelper output) : base(output)
    {
    }

    private static string name = ChatCompletionClient.GetAllowedUniqueName(DateTime.UtcNow.ToString());

    private static string defaultJsonSchema = @"{
  ""name"": """ + name + @""",
  ""strict"": true,
  ""schema"": {
    ""type"": ""object"",
    ""properties"": {
      ""Blocked"": {
        ""type"": ""boolean""
      },
      ""Reason"": {
        ""type"": ""string"",
        ""description"": ""Concise reason for why this comment was marked as spam or ham""
      }
    },
    ""required"": [
      ""Blocked"",
      ""Reason""
    ],
    ""additionalProperties"": false
  }
}";


    [RavenTheory(RavenTestCategory.Ai, Skip = "Consume tokens for all other tests")]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Skip = "Stress test")]
    // Ollama Doesn't throw
    public async Task RateLimit_MaxTokens(Options options, GenAiConfiguration configuration)
    {
        using (var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests())))
        using (var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, configuration.Connection))
        {
            var prompt = "Check if the following blog post comment is spam or not";
            var context =
                "{\"Text\":\"Surefire investment property in caiman islands, win $$$$ for sure, qucik!\",\"Author\":\"homepage\",\"Id\":\"2236672c-b941-4855-999e-5374f41cbddd\"}";

            var sb = new StringBuilder();

            sb.Clear();
            for (int i = 0; i < 50_000; i++)
            {
                sb.Append(context);
            }

            context = sb.ToString();

            await Assert.ThrowsAsync<TooManyTokensException>(() => client.TestCompleteAsync(prompt, context, defaultJsonSchema, default));
        }
    }

    [RavenTheory(RavenTestCategory.Ai, Skip = "Consume tokens for all other tests")]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Skip = "Stress test")]
    // Ollama Doesn't throw
    public async Task RateLimit_ByHighRequestFreq(Options options, GenAiConfiguration configuration)
    {
        var prompt = "Check if the following blog post comment is spam or not";
        var context = "{\"Text\":\"Surefire investment property in caiman islands, win $$$$ for sure, qucik!\",\"Author\":\"homepage\",\"Id\":\"2236672c-b941-4855-999e-5374f41cbddd\"}";

        using var contextPool = new TransactionContextPool(RavenLogManager.Instance.CreateNullLogger(), new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()));
        using var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, configuration.Connection);

        //Raven.Server.Documents.AI.AiGen.GenAiRateLimitException: Rate limit reached for gpt-4o in organization "..." on requests per min (RPM): Limit 500, Used 500, Requested 1. Please try again in 120ms.
        await Assert.ThrowsAsync<RateLimitException>(async () =>
        {
            var tasks = new List<Task>();
            for (int i = 0; i < 20_000; i++)
            {
                var t = client.TestCompleteAsync(prompt, context, defaultJsonSchema, default);
                tasks.Add(t);
            }
            await Task.WhenAll(tasks);
        });
    }
}

