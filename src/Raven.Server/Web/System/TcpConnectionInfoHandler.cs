using System;
using System.Threading.Tasks;
using Raven.Client.Server.Commands;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class TcpConnectionInfoHandler : RequestHandler
    {
        [RavenAction("/info/tcp", "GET", "tcp-connections-state", NoAuthorizationRequired = true)]
        public Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var tcpServerUrl = Server.ServerStore.NodeTcpServerUrl;
                if (tcpServerUrl.StartsWith("tcp://localhost.fiddler:", StringComparison.OrdinalIgnoreCase))
                    tcpServerUrl = tcpServerUrl.Remove(15, 8);

                var output = new DynamicJsonValue
                {
                    [nameof(TcpConnectionInfo.Url)] = tcpServerUrl,
                    [nameof(TcpConnectionInfo.Certificate)] = Server.ServerCertificateHolder.CertificateForClients,
                };

                context.Write(writer, output);
            }

            return Task.CompletedTask;
        }
    }
}