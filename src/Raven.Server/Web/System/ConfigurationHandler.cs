using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Server.Operations.Configuration;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class ConfigurationHandler : RequestHandler
    {
        [RavenAction("/configuration/client", "GET")]
        public Task GetClientConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clientConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out long index);
                    if (clientConfigurationJson == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.RaftCommandIndex));
                        writer.WriteInteger(index);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(GetClientConfigurationOperation.Result.Configuration));
                        writer.WriteObject(clientConfigurationJson);

                        writer.WriteEndObject();
                    }
                }
            }

            return Task.CompletedTask;
        }
    }
}