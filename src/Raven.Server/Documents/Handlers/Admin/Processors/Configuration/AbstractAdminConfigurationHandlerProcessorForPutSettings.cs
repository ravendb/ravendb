using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForPutSettings<TOperationContext> : AbstractAdminConfigurationHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext 
{
    protected AbstractAdminConfigurationHandlerProcessorForPutSettings(AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
        {
            var databaseSettingsJson = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), Constants.DatabaseSettings.StudioId);

            Dictionary<string, string> settings = new Dictionary<string, string>();
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < databaseSettingsJson.Count; i++)
            {
                databaseSettingsJson.GetPropertyByIndex(i, ref prop);
                settings.Add(prop.Name, prop.Value?.ToString());
            }

            await UpdateDatabaseRecordAsync(context, (record, _) => record.Settings = settings, RequestHandler.GetRaftRequestIdFromQuery(), RequestHandler.DatabaseName);
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
    }
}
