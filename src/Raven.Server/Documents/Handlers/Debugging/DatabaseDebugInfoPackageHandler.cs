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

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DatabaseDebugInfoPackageHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/info-package", "GET", IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {
            var contentDisposition = $"attachment; filename=debug-info of {Database.Name} {DateTime.UtcNow}.zip";
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
                            { "database",new StringValues(Database.Name) },
                        };

                        foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.Databases))
                        {
                            var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, null);
                            try
                            {
                                var entry = archive.CreateEntry(entryName);
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
                                WriteExceptionAsZipEntry(e,archive,entryName.Replace(".json", string.Empty));
                            }
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        private static void WriteExceptionAsZipEntry(Exception e, ZipArchive archive, string entryName)
        {
            if (entryName.EndsWith(".json"))
            {
                var index = entryName.LastIndexOf(".json", StringComparison.OrdinalIgnoreCase);
                entryName = entryName.Substring(0, index);
            }

            var entry = archive.CreateEntry($"{entryName}.error");
            using (var entryStream = entry.Open())
            using (var sw = new StreamWriter(entryStream))
            {
                sw.Write(e);
                sw.Flush();
            }
        }

    }
}
