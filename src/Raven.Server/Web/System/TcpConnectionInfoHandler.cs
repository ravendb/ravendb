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
            using(ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                DynamicJsonValue output;
                try
                {
                    var ip = await Server.GetTcpServerPort();
                    output = new DynamicJsonValue
                    {
                        ["Port"] = ip.Port,
                        ["Url"] = Server.Configuration.Core.TcpServerUrl
                    };
                }
                catch (Exception e)
                {
                    output = new DynamicJsonValue
                    {
                        ["Type"] = "Error",
                        ["Exception"] = e.ToString()
                    };
                }
               
                context.Write(writer, output);
            }
        }
    }
}