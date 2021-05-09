using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class JsonPatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/json-patch", "PATCH", AuthorizationStatus.ValidUser)]
        public async Task DocOperations()
        {
            var id = GetStringQueryString("id");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "json-patch");
                if (blittable == null)
                    throw new BadRequestException("Missing JSON content.");

                var commands = JsonPatchCommand.Parse(blittable);
                var jsonPatchCommand = new JsonPatchCommand(id, commands, returnDocument: false, context);
                await Database.TxMerger.Enqueue(jsonPatchCommand);
            }
        }

        
    }
}
