using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ConfigurationHandler : DatabaseRequestHandler
    {   
        [RavenAction("/databases/*/configuration/client", "GET", AuthorizationStatus.ValidUser)]
        public Task GetClientConfiguration()
        {
            var inherit = GetBoolValueQueryString("inherit", required: false) ?? true;

            var configuration = Database.ClientConfiguration;
            long etag = configuration?.Etag ?? -1;
            var serverConfiguration = GetServerClientConfiguration(out long serverIndex);
            etag = Hashing.Combine(etag, serverConfiguration?.Etag ?? -2);
            if (inherit)
            {
            
                if (configuration == null || configuration.Disabled)
                {
                    if (serverConfiguration != null)
                    {
                        configuration = serverConfiguration;
                        etag = serverIndex;
                    }
                }
            }

            if (configuration == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var clientConfigurationJson = context.ReadObject(configuration.ToJson(), Constants.Configuration.ClientId);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.Etag));
                    writer.WriteInteger(etag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.Configuration));
                    writer.WriteObject(clientConfigurationJson);

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        private ClientConfiguration GetServerClientConfiguration(out long index)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clientConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out index);
                    return clientConfigurationJson != null
                        ? JsonDeserializationServer.ClientConfiguration(clientConfigurationJson)
                        : null;
                }
            }
        }
    }
}