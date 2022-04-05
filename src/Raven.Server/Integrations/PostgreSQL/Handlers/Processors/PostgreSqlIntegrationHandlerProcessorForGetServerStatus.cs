using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal class PostgreSqlIntegrationHandlerProcessorForGetServerStatus<TOperationContext> : AbstractPostgreSqlIntegrationHandlerProcessor<RequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public PostgreSqlIntegrationHandlerProcessorForGetServerStatus([NotNull] RequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        AssertCanUsePostgreSqlIntegration(RequestHandler);

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            var dto = new PostgreSqlServerStatus { Active = RequestHandler.Server.PostgresServer.Active };

            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(dto);
            writer.WriteObject(context.ReadObject(djv, "PostgreSqlServerStatus"));
        }
    }
}
