using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.SampleData;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio
{
    public class SampleDataHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/sample-data", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task PostCreateSampleData()
        {
            using (var processor = new SampleDataHandlerProcessorForPostSampleData(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/studio/sample-data/classes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSampleDataClasses()
        {
            using (var processor = new SampleDataHandlerProcessorForGetSampleDataClasses(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}

