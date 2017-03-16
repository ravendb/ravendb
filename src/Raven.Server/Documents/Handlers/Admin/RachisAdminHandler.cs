using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class RachisAdminHandler : AdminRequestHandler
    {

        [RavenAction("/rachis/send", "POST", "/rachis/send")]
        public async Task ApplyCommand()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var command = await context.ReadForMemoryAsync(RequestBodyStream(), "ExternalRachisCommand");

                string type;
                if(command.TryGet("Type",out type) == false)
                {
                    // todo: maybe addd further validation?
                    throw new ArgumentException("Recieved command must contain a Type field");
                }

                await ServerStore.PutCommandAsync(command);
            }
        }
    }
}