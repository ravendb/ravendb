using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal class SmugglerHandlerProcessorForImport : AbstractSmugglerHandlerProcessorForImport<SmugglerHandler, DocumentsOperationContext>
    {
        public SmugglerHandlerProcessorForImport([NotNull] SmugglerHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ImportAsync(JsonOperationContext context, long? operationId)
        {
            operationId ??= RequestHandler.Database.Operations.GetNextOperationId();
            await Import(context, RequestHandler.Database.Name, RequestHandler.DoImportInternalAsync, RequestHandler.Database.Operations, operationId.Value);
        }
    }
}
