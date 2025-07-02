using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.AI.GenAi
{
    internal class OpenAiChatCompletionClient : AbstractChatCompletionClient<TransactionOperationContext>
    {
        public OpenAiChatCompletionClient(GenAiConfiguration configuration, string structuredOutputSchema, TransactionContextPool contextPool, DocumentConventions conventions)
            : base(
                baseUri: new Uri(configuration.Connection.OpenAiSettings.Endpoint),
                model: configuration.Connection.OpenAiSettings.Model,
                apiKey: configuration.Connection.OpenAiSettings.ApiKey,
                organizationId: configuration.Connection.OpenAiSettings.OrganizationId,
                projectId: configuration.Connection.OpenAiSettings.ProjectId,
                structuredOutputSchema,
                contextPool, conventions)
        {
        }
    }

    internal class OllamaChatCompletionClient : AbstractChatCompletionClient<TransactionOperationContext>
    {
        private readonly OllamaSettings _ollamaSettings;

        public OllamaChatCompletionClient(GenAiConfiguration configuration, string structuredOutputSchema, TransactionContextPool contextPool, DocumentConventions conventions)
            : base(
                baseUri: new Uri(configuration.Connection.OllamaSettings.Uri),
                model: configuration.Connection.OllamaSettings.Model,
                apiKey: null,
                organizationId: null,
                projectId: null,
                structuredOutputSchema,
                contextPool,
                conventions)
        {
            _ollamaSettings = configuration.Connection.OllamaSettings;
        }

        /// <summary>
        /// Override to add Ollama-specific parameters like "think" to the request payload.
        /// The "think" parameter controls whether thinking models engage their reasoning process.
        /// Setting think=false skips reasoning entirely, providing faster responses and lower token usage.
        /// </summary>
        protected override void WriteCustomParameters(AsyncBlittableJsonTextWriter writer)
        {
            // Add Ollama-specific "think" parameter if specified
            if (_ollamaSettings.Think.HasValue)
            {
                writer.WriteComma();
                writer.WritePropertyName("think");
                writer.WriteBool(_ollamaSettings.Think.Value);
            }
        }
    }

    public class GenericChatCompletionClientForTesting : AbstractChatCompletionClient<TransactionOperationContext>
    {
        public GenericChatCompletionClientForTesting(string uri, string model, string apiKey, string organizationId, string projectId, JsonContextPoolBase<TransactionOperationContext> contextPool)
            : base(new Uri(uri), model, apiKey, organizationId, projectId, structuredOutputSchema: null, contextPool, conventions: DocumentConventions.DefaultForServer)
        {
        }
    }
}
