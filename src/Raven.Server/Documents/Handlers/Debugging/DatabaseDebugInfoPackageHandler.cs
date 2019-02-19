using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DatabaseDebugInfoPackageHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/info-package", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {
           
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Database [{Database.Name}].zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        var endpointParameters = new Dictionary<string, StringValues>
                        {
                            { "database",new StringValues(Database.Name) }
                        };

                        foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.Databases))
                        {
                            var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, null);
                            try
                            {
                                var entry = archive.CreateEntry(entryName);
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                using (var entryStream = entry.Open())
                                using (var writer = new BlittableJsonTextWriter(context, entryStream))
                                {
                                    using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route, context, endpointParameters))
                                    {
                                        context.Write(writer, endpointOutput);
                                        writer.Flush();
                                        await entryStream.FlushAsync();
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                DebugInfoPackageUtils.WriteExceptionAsZipEntry(e,archive,entryName.Replace(".json", string.Empty));
                            }
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }
    }
}
