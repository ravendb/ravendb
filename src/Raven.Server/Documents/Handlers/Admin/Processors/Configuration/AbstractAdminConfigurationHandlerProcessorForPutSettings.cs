using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForPutSettings<TRequestHandler, TOperationContext> : AbstractAdminConfigurationHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractAdminConfigurationHandlerProcessorForPutSettings(TRequestHandler requestHandler, JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var databaseSettingsJson = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), Constants.DatabaseSettings.StudioId);

            Dictionary<string, string> settings = new Dictionary<string, string>();
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < databaseSettingsJson.Count; i++)
            {
                databaseSettingsJson.GetPropertyByIndex(i, ref prop);
                settings.Add(prop.Name, prop.Value?.ToString());
            }

            await UpdateDatabaseRecordAsync(context, (record, _) => record.Settings = settings, RequestHandler.GetRaftRequestIdFromQuery(), GetDatabaseName());
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
    }
}
