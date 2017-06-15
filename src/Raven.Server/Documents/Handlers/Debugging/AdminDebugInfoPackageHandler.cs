using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Routing;
using Raven.Server.ServerWide.DebugInfo;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class AdminDebugInfoPackageHandler : AdminRequestHandler
    {
        private static readonly IReadOnlyList<RouteInformation> _debugInformationRoutes = 
            RouteScanner.Scan(attr => attr.IsDebugInformationEndpoint && 
                                      attr.Path.Contains("info-package") == false).Values.ToList();

        [RavenAction("/debug/info-package", "GET", IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {            
            var contentDisposition = $"attachment; filename={DateTime.UtcNow}.debug-info-package.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        await WriteServerWide(archive, context);
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        private async Task WriteServerWide(ZipArchive archive, JsonOperationContext context)
        {
            var serverwideDataSources = GetServerwideDataSources();
            try
            {
                //theoretically this could be parallelized,
                //however ZipArchive allows only one archive entry to be open concurrently
                foreach (var dataSource in serverwideDataSources)
                {
                    var entry = archive.CreateEntry(dataSource.FullPath);
                    using (var entryStream = entry.Open())
                    using (var writer = new BlittableJsonTextWriter(context, entryStream))
                    using (var endpointOutput = await dataSource.GetData(context))
                    {
                        context.Write(writer, endpointOutput);
                        writer.Flush();
                        await entryStream.FlushAsync();
                    }
                }
            }
            finally
            {
                foreach (var dataSource in serverwideDataSources)
                {
                    dataSource.Dispose();
                }
            }
        }

        public IReadOnlyList<IDebugInfoDataSource> GetServerwideDataSources()
        {
            var dataSources = new List<IDebugInfoDataSource>();

            foreach (var route in _debugInformationRoutes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.None))
            {
                dataSources.Add(new ServerEndpointDebugInfoDataSource(ServerStore.NodeHttpServerUrl, route, ServerStore.ServerShutdown));
            }

            return dataSources;
        }
    }
}
