using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerWideDebugInfoPackageHandler : AdminRequestHandler
    {
        [RavenAction("/debug/info-package", "GET", IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {              
            var contentDisposition = $"attachment; filename=Server wide debug-info {DateTime.UtcNow}.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        await WriteServerWide(archive, context, localEndpointClient);
                        await WritePerDatabase(archive, context, localEndpointClient);
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        private async Task WriteServerWide(ZipArchive archive, JsonOperationContext context, LocalEndpointClient localEndpointClient)
        {
            //theoretically this could be parallelized,
            //however ZipArchive allows only one archive entry to be open concurrently
            foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.None))
            {
                var entry = archive.CreateEntry(DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route,"server-wide"));
                using (var entryStream = entry.Open())
                using (var writer = new BlittableJsonTextWriter(context, entryStream))
                using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route,context))
                {
                    context.Write(writer, endpointOutput);
                    writer.Flush();
                    await entryStream.FlushAsync();
                }
            }
        }

        private async Task WritePerDatabase(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {
                foreach (var databaseName in ServerStore.Cluster.GetDatabaseNames(transactionOperationContext))
                {
                    var endpointParameters = new Dictionary<string, StringValues>
                    {
                        { "database",new StringValues(databaseName) },
                    };

                    foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.Databases))
                    {
                        var entry = archive.CreateEntry(DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, databaseName));
                        using (var entryStream = entry.Open())
                        using (var writer = new BlittableJsonTextWriter(jsonOperationContext, entryStream))
                        {
                            using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route, jsonOperationContext, endpointParameters))
                            {
                                jsonOperationContext.Write(writer, endpointOutput);
                                writer.Flush();
                                await entryStream.FlushAsync();
                            }
                        }
                    }
                }
            }
        }        
    }
}
