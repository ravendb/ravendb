using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class TcpConnectionInfoHandler : RequestHandler
    {
        [RavenAction("/info/tcp", "GET", "tcp-connections-state", NoAuthorizationRequired = true)]
        public async Task Get()
        {
            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var port = await Server.GetTcpServerPortAsync();
                var output = new DynamicJsonValue
                {
                    ["Port"] = port,
                    ["Url"] = Server.Configuration.Core.TcpServerUrl
                };

                context.Write(writer, output);
            }
        }
    }
}