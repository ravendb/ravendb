using System;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal sealed class RevisionsHandlerProcessorForRevertRevisions : AbstractRevisionsHandlerProcessorForRevertRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForRevertRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void ScheduleRevertRevisions(long operationId, RevertRevisionsRequest configuration, OperationCancelToken token)
        {
            var schemaValidationCache = RequestHandler.Database.SchemaValidatorCache;

            if (schemaValidationCache is { Disabled: false } && schemaValidationCache.IsSchemaEnabledForAny(configuration.Collections))
                throw new InvalidOperationException("Reverting documents to revisions is not allowed when Schema Validation is enabled. Please disable Schema Validation and try again.");

            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseRevert,
                $"Revert database '{RequestHandler.Database.Name}' to {configuration.Time} UTC.",
                detailedDescription: null,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.RevertRevisions(
                    configuration, onProgress, token),
                token: token);

            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }
    }
}
