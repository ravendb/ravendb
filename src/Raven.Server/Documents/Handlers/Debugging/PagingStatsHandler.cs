using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging;

public class PagingStatsHandler : ServerRequestHandler
{
    [RavenAction("/debug/storage/paging/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
    public async Task PagingStatistics()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
        {
            var t = Voron.Impl.Paging.PagingStatistics.GetTotals();
            context.Write(writer, new DynamicJsonValue
            {
                [nameof(t.TotalReads)] = t.TotalReads,
                [nameof(t.TotalWrites)] = t.TotalWrites
            });
        }
    }
}
