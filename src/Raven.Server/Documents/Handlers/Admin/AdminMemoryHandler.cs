using System;
using System.Runtime;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminMemoryHandler : RequestHandler
    {
        [RavenAction("/admin/memory/gc", "GET", AuthorizationStatus.Operator)]
        public Task CollectGarbage()
        {
            var loh = GetBoolValueQueryString("loh", required: false) ?? false;

            if (loh)
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);

            return Task.CompletedTask;
        }
    }
}
