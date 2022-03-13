using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio
{
    public class SampleDataHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/sample-data", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task PostCreateSampleData()
        {
            using (var processor = new SampleDataHandlerProcessorForPostSampleData<DatabaseRequestHandler, DocumentsOperationContext>(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/studio/sample-data/classes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSampleDataClasses()
        {
            await using (var sampleData = typeof(SampleDataHandler).Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.NorthwindModel.cs"))
            await using (var responseStream = ResponseBodyStream())
            {
                HttpContext.Response.ContentType = "text/plain";
                await sampleData.CopyToAsync(responseStream);
            }
        }
    }
}

