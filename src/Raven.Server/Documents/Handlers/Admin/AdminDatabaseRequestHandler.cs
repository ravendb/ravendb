using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    
    public abstract class AdminDatabaseRequestHandler : DatabaseRequestHandler
    {
        // TODO : implement "admin" part
    }

    public abstract class AdminRequestHandler : RequestHandler
    {
        
        // TODO : implement "sys admin" part
    }

    public class RachisAdminHandler : AdminRequestHandler
    {
        private static readonly StringSegment TypeSegment = new StringSegment("Type");

        [RavenAction("/rachis/apply", "POST", "/rachis/apply")]
        public async Task ApplyCommand()
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var command = await context.ReadForDiskAsync(RequestBodyStream(), "ExternalRachisCommand");

                if (command.Contains(context.GetLazyStringForFieldWithCaching(TypeSegment)) == false)
                {
                    // todo: maybe addd further validation?
                    throw new ArgumentException("Recieved command must contain a Type field");
                }

                await ServerStore.SendToLeaderAsync(command);
            }
        }
    }
}