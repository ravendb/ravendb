using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DatabaseDebugInfoPackageHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/info-package", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Database [{Database.Name}].zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        var endpointParameters = new Dictionary<string, Microsoft.Extensions.Primitives.StringValues>
                        {
                            { "database",new Microsoft.Extensions.Primitives.StringValues(Database.Name) }
                        };
                        
                        var routes = DebugInfoPackageUtils.GetAuthorizedRoutes(Server, HttpContext, Database.Name)
                            .Where(x => x.TypeOfRoute == RouteInformation.RouteType.Databases);

                        foreach (var route in routes)
                        {
                            await ServerWideDebugInfoPackageHandler.InvokeAndWriteToArchive(archive, context, localEndpointClient, route, null, endpointParameters);
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }
    }
}
