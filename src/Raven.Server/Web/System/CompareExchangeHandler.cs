using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.System.Processors;

namespace Raven.Server.Web.System
{
    internal class CompareExchangeHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/cmpxchg", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetCompareExchangeValues()
        {
            using (var processor = new CompareExchangeHandlerProcessorForGetCompareExchangeValues(this, Database.Name))
                await processor.ExecuteAsync();
        }


        [RavenAction("/databases/*/cmpxchg", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PutCompareExchangeValue()
        {
            using (var processor = new CompareExchangeHandlerProcessorForPutCompareExchangeValue(this, Database.Name))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/cmpxchg", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task DeleteCompareExchangeValue()
        {
            using (var processor = new CompareExchangeHandlerProcessorForDeleteCompareExchangeValue(this, Database.Name))
                await processor.ExecuteAsync();
        }
    }
}
