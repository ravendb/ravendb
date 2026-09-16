using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.RelationalWriters;
using Raven.Server.Logging;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.Handlers.Processors;

internal static class SqlEtlTestConnectionHelper
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(SqlEtlTestConnectionHelper));

    public static async Task ExecuteAsync(RequestHandler requestHandler)
    {
        try
        {
            var factoryName = requestHandler.GetStringQueryString("factoryName");
            var connectionString = await new StreamReader(requestHandler.HttpContext.Request.Body).ReadToEndAsync();
            SqlDatabaseWriter.TestConnection(factoryName, connectionString);

            DynamicJsonValue result = new()
            {
                [nameof(NodeConnectionTestResult.Success)] = true,
            };

            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
        catch (Exception ex)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Error occurred during sql replication connection test", ex);

            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
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
