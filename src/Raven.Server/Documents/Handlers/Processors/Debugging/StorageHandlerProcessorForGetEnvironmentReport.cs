using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Debugging;

namespace Raven.Server.Documents.Handlers.Processors.Debugging;

internal class StorageHandlerProcessorForGetEnvironmentReport : AbstractStorageHandlerProcessorForGetEnvironmentReport<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StorageHandlerProcessorForGetEnvironmentReport([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var type = GetEnvironmentType();
        var details = GetDetails();

        var env = RequestHandler.Database.GetAllStoragesEnvironment()
            .FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) && x.Type == type);

        if (env == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Name");
                writer.WriteString(env.Name);
                writer.WriteComma();

                writer.WritePropertyName("Type");
                writer.WriteString(env.Type.ToString());
                writer.WriteComma();

                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetDetailedReport(env, details));
                writer.WritePropertyName("Report");
                writer.WriteObject(context.ReadObject(djv, env.Name));

                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    private DetailedStorageReport GetDetailedReport(StorageEnvironmentWithType environment, bool details)
    {
        if (environment.Type != StorageEnvironmentWithType.StorageEnvironmentType.Index)
        {
            using (var tx = environment.Environment.ReadTransaction())
            {
                return environment.Environment.GenerateDetailedReport(tx, details);
            }
        }

        var index = RequestHandler.Database.IndexStore.GetIndex(environment.Name);
        return index.GenerateStorageReport(details);
    }
}
