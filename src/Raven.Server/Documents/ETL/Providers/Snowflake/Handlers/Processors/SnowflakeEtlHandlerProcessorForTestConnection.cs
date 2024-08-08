using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.ETL.Providers.Snowflake.RelationalWriters;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Snowflake.Handlers.Processors;

internal sealed class SnowflakeEtlHandlerProcessorForTestConnection<TOperationContext>([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
    : AbstractDatabaseHandlerProcessor<TOperationContext>(requestHandler)
    where TOperationContext : JsonOperationContext
{
    public override async ValueTask ExecuteAsync()
    {
        try
        {
            var connectionString = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
            SnowflakeDatabaseWriter.TestConnection(connectionString);

            DynamicJsonValue result = new()
            {
                [nameof(NodeConnectionTestResult.Success)] = true,
            };

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
        catch (Exception ex)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Error occurred during snowflake replication connection test", ex);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = false,
                        [nameof(NodeConnectionTestResult.Error)] = ex.ToString()
                    });
                }
            }
        }
    }
}
