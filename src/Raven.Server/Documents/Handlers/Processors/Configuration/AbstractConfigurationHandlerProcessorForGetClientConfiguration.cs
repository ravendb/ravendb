using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Configuration;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Configuration;

internal abstract class AbstractConfigurationHandlerProcessorForGetClientConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractConfigurationHandlerProcessorForGetClientConfiguration([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract ClientConfiguration GetDatabaseClientConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var inherit = RequestHandler.GetBoolValueQueryString("inherit", required: false) ?? true;

        var configuration = GetDatabaseClientConfiguration();
        var serverConfiguration = GetServerClientConfiguration();

        if (inherit && (configuration == null || configuration.Disabled) && serverConfiguration != null)
        {
            configuration = serverConfiguration;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            BlittableJsonReaderObject clientConfigurationJson = null;
            if (configuration != null)
            {
                var val = configuration.ToJson();
                clientConfigurationJson = context.ReadObject(val, Constants.Configuration.ClientId);
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.Etag));
                writer.WriteInteger(ClientConfigurationHelper.GetClientConfigurationEtag(configuration, RequestHandler.ServerStore));
                writer.WriteComma();

                writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.Configuration));
                if (clientConfigurationJson != null)
                {
                    writer.WriteObject(clientConfigurationJson);
                }
                else
                {
                    writer.WriteNull();
                }

                writer.WriteEndObject();
            }
        }
    }

    private ClientConfiguration GetServerClientConfiguration()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (context.OpenReadTransaction())
            {
                var clientConfigurationJson = RequestHandler.ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out _);
                var config = clientConfigurationJson != null
                    ? JsonDeserializationServer.ClientConfiguration(clientConfigurationJson)
                    : null;

                return config;
            }
        }
    }
}
