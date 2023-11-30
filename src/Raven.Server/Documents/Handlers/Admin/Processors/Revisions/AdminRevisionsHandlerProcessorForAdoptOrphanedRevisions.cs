using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            Description = $"Adopt orphaned revisions in database '{RequestHandler.DatabaseName}'.";
        }

        protected override AdoptOrphanedRevisionsOperation.Parameters GetOperationParameters(BlittableJsonReaderObject json)
        {
            return JsonDeserializationServer.Parameters.AdoptOrphanedRevisionsConfigurationOperationParameters(json);
        }

        protected override Task<IOperationResult> ExecuteOperation(Action<IOperationProgress> onProgress, AdoptOrphanedRevisionsOperation.Parameters parameters, OperationCancelToken token)
        {
            return RequestHandler.Database.DocumentsStorage.RevisionsStorage.AdoptOrphanedAsync(onProgress, parameters.Collections.ToHashSet(), token);
        }
    }
}
