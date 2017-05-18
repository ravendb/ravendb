using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/debug/runaway", "GET")]
        public Task RunawayThreads()
        {            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var write = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(write, new DynamicJsonArray(ThreadTimings.GetRunawayThreads()
                                                                           .OrderByDescending(x => x.TotalTimeMilliseconds)
                                                                           .Select(ti => ti.ToJson())));
                    write.Flush();
                }
            }
            return Task.CompletedTask;
        }
    }
}
