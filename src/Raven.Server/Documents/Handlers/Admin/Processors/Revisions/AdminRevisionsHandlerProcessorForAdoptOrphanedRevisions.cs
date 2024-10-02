using System;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal class AdminRevisionsHandlerProcessorForAdoptOrphanedRevisions : AdminRevisionsHandlerProcessorForRevisionsOperation<AdoptOrphanedRevisionsOperation.Parameters>
    {
        public AdminRevisionsHandlerProcessorForAdoptOrphanedRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, OperationType.AdoptOrphanedRevisions)
        {
        }

        public override string Description => $"Adopt orphaned revisions in database '{RequestHandler.DatabaseName}'.";

        protected override AdoptOrphanedRevisionsOperation.Parameters GetOperationParameters(BlittableJsonReaderObject json)
        {
            var parameters = JsonDeserializationServer.Parameters.AdoptOrphanedRevisionsConfigurationOperationParameters(json);
            parameters.Collections = parameters.Collections?.Length > 0 ? parameters.Collections : null;
            return parameters;
        }

        protected override Task<IOperationResult> ExecuteOperation(Action<IOperationProgress> onProgress, AdoptOrphanedRevisionsOperation.Parameters parameters, OperationCancelToken token)
        {
            return RequestHandler.Database.DocumentsStorage.RevisionsStorage.AdoptOrphanedAsync(onProgress, parameters.Collections?.ToHashSet(), token);
        }
    }
}
