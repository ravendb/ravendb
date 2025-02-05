using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal abstract class AiEtlHandlerProcessorForTestAiConnectionBase<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private protected string JsonConfigString;

    public AiEtlHandlerProcessorForTestAiConnectionBase([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        IServiceProvider services = null;
        try
        {
            JsonConfigString = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
            (AiConnectorType aiConnectorType, AiConnectionString connection) = GetAiConnectorDetails();
            var aiEtlConfiguration = new AiEtlConfiguration { AiConnectorType = aiConnectorType, Connection = connection };

            services = AiHelper.CreateServicesForTest(aiEtlConfiguration, out string serviceId);
            var embeddings = await services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId).GenerateEmbeddingsAsync(AiHelper.TestValuesList);

            if (embeddings.Count != AiHelper.TestValuesList.Count)
                throw new Exception($"Failed to generate embeddings for test values. Expected '{AiHelper.TestValuesList.Count}' result, but got '{embeddings.Count}'.");

            var result = new DynamicJsonValue { [nameof(NodeConnectionTestResult.Success)] = true };

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
        catch (Exception e)
        {
            var result = new DynamicJsonValue
            {
                [nameof(NodeConnectionTestResult.Success)] = false,
                [nameof(NodeConnectionTestResult.Error)] = e.ToString()
            };

            if (services != null)
            {
                var logger = (InMemoryLoggerProvider)services.GetRequiredService<ILoggerProvider>();
                var logsArray = new DynamicJsonArray(collection: logger.GetLogs());

                result[nameof(NodeConnectionTestResult.Log)] = logsArray;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
        finally
        {
            (services?.GetRequiredService<ILoggerProvider>() as InMemoryLoggerProvider)?.Dispose();
        }
    }

    public abstract (AiConnectorType AiConnectorType, AiConnectionString Connection) GetAiConnectorDetails();
}
