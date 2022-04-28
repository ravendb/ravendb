using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes
{
    internal class AdminIndexHandlerProcessorForDump : AbstractAdminIndexHandlerProcessorForDump<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminIndexHandlerProcessorForDump([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask ExecuteForCurrentNodeAsync()
        {
            var (name, path) = GetParameters();

            var index = RequestHandler.Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                IndexDoesNotExistException.ThrowFor(name); //never hit
                return;
            }

            var operationId = RequestHandler.Database.Operations.GetNextOperationId();
            var token = RequestHandler.CreateTimeLimitedQueryOperationToken();

            _ = RequestHandler.Database.Operations.AddOperation(
                RequestHandler.Database,
                "Dump index " + name + " to " + path,
                Operations.Operations.OperationType.DumpRawIndexData,
                onProgress =>
                {
                    var totalFiles = index.Dump(path, onProgress);
                    return Task.FromResult((IOperationResult)new AdminIndexHandler.DumpIndexResult
                    {
                        Message = $"Dumped {totalFiles} files from {name}",
                    });
                }, operationId, token: token);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, RequestHandler.ServerStore.NodeTag);
            }
        }

        protected override Task ExecuteForRemoteNodeAsync(RavenCommand command) => RequestHandler.ExecuteRemoteAsync(command);
    }
}
