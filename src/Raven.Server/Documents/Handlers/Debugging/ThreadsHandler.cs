using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ThreadsHandler : RequestHandler
    {
        [RavenAction("/admin/debug/threads/runaway", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task RunawayThreads()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    try
                    {
                        var threadsUsage = new ThreadsUsage();

                        // need to wait to get a correct measure of the cpu
                        await Task.Delay(100);

                        var result = threadsUsage.Calculate();
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Runaway Threads"] = result.ToJson()
                            });
                        
                    }
                    catch (Exception e)
                    {
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Error"] = e.ToString()
                            });
                    }

                    writer.Flush();
                }
            }
        }
    }
}
