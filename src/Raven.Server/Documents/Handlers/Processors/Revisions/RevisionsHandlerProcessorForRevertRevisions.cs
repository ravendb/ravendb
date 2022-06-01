using System;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForRevertRevisions : AbstractRevisionsHandlerProcessorForRevertRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForRevertRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void RevertRevisions(long operationId, RevertRevisionsRequest configuration, OperationCancelToken token)
        {
            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseRevert,
                $"Revert database '{RequestHandler.Database.Name}' to {configuration.Time} UTC.",
                detailedDescription: null,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.RevertRevisions(configuration.Time, TimeSpan.FromSeconds(configuration.WindowInSec), onProgress, token),
                token: token);
        }

        protected override long GetNextOperationId()
        {
            return ServerStore.Operations.GetNextOperationId();
        }
    }
}
