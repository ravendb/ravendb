using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Raven.Server.Web.Studio.Sharding.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ConfigurationHandler : DatabaseRequestHandler
    {
        internal abstract class AbstractStudioConfigurationHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
            where TRequestHandler : RequestHandler
            where TOperationContext : JsonOperationContext
        {
            protected AbstractStudioConfigurationHandlerProcessor(TRequestHandler requestHandler, JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
            { }

            public async Task WriteStudioConfiguration(StudioConfiguration studioConfiguration)
            {
                if (studioConfiguration == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var val = studioConfiguration.ToJson();
                    var clientConfigurationJson = context.ReadObject(val, Constants.Configuration.StudioId);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        writer.WriteObject(clientConfigurationJson);
                    }
                }
            }
        }

        internal class StudioConfigurationHandlerProcessor : AbstractStudioConfigurationHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
        {
            public StudioConfigurationHandlerProcessor(DatabaseRequestHandler requestHandler, DocumentsContextPool contextPool) : base(requestHandler, contextPool) { }
        }

        [RavenAction("/databases/*/configuration/studio", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetStudioConfiguration()
        {
            var configuration = Database.StudioConfiguration;
            using (var processor = new StudioConfigurationHandlerProcessor(this, ContextPool))
            {
                await processor.WriteStudioConfiguration(configuration);
            }
        }

        [RavenAction("/databases/*/configuration/client", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetClientConfiguration()
        {
            var inherit = GetBoolValueQueryString("inherit", required: false) ?? true;

            var configuration = Database.ClientConfiguration;
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

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.Etag));
                    writer.WriteInteger(Database.GetClientConfigurationEtag());
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
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clientConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out _);
                    var config = clientConfigurationJson != null
                        ? JsonDeserializationServer.ClientConfiguration(clientConfigurationJson)
                        : null;

                    return config;
                }
            }
        }
    }
}
