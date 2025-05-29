using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.GenAi
{
    internal class OpenAiChatCompletionClient : AbstractChatCompletionClient<TransactionOperationContext>
    {
        public OpenAiChatCompletionClient(GenAiConfiguration configuration, TransactionContextPool contextPool, DocumentConventions conventions) : base(baseUri: new Uri(configuration.Connection.OpenAiSettings.Endpoint),
            model: configuration.Connection.OpenAiSettings.Model, apiKey: configuration.Connection.OpenAiSettings.ApiKey,
            structuredOutputSchema: configuration.JsonSchema, contextPool, conventions)
        {
        }
    }

    internal class OllamaChatCompletionClient : AbstractChatCompletionClient<TransactionOperationContext>
    {
        public OllamaChatCompletionClient(GenAiConfiguration configuration, TransactionContextPool contextPool, DocumentConventions conventions) : base(baseUri: new Uri(configuration.Connection.OllamaSettings.Uri),
            model: configuration.Connection.OllamaSettings.Model, apiKey: null, structuredOutputSchema: configuration.JsonSchema, contextPool, conventions)
        {
        }
    }

    public class GenericChatCompletionClientForTesting : AbstractChatCompletionClient<TransactionOperationContext>
    {
        public GenericChatCompletionClientForTesting(string uri, string model, string apiKey, JsonContextPoolBase<TransactionOperationContext> contextPool) : base(new Uri(uri), model, apiKey, structuredOutputSchema: null, contextPool, conventions: DocumentConventions.DefaultForServer)
        {
        }
    }
}
