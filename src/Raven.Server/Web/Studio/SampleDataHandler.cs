using System;
using System.IO.Compression;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;

namespace Raven.Server.Studio.Handlers
{
    public class SampleDataHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/studio/sampleData", "POST")]
        public async Task PostCreateSampleData()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (context.OpenReadTransaction())
                {
                    //TODO: verify if it works properly when creating empty database with bundles configuration - maybe we should find info about collections? Database.DocumentsStorage.GetCollections(context)
                    var documentCount = Database.DocumentsStorage.GetNumberOfDocuments(context);

                    if (documentCount > 0)
                    {
                        throw new InvalidOperationException("You cannot create sample data in a database that already contains documents");
                    }
                }

                using (var sampleData = typeof(SampleDataHandler).GetTypeInfo().Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.Northwind_3.5.35168.ravendbdump"))
                {
                    using (var stream = new GZipStream(sampleData, CompressionMode.Decompress))
                    {
                        var importer = new SmugglerImporter(Database);
                        importer.OperateOnTypes = DatabaseItemType.Documents; //TODO: remove this line after we get support for indexing

                        await importer.Import(context, stream);
                    }
                }
            }
        }

        [RavenAction("/databases/*/studio/sampleDataClasses", "GET")]
        public async Task GetSampleDataClasses()
        {
            using (var sampleData = typeof(SampleDataHandler).GetTypeInfo().Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.NorthwindModel.cs"))
            using (var responseStream = ResponseBodyStream())
            {
                HttpContext.Response.ContentType = "text/plain";
                await sampleData.CopyToAsync(responseStream);
            }
        }
    }
}