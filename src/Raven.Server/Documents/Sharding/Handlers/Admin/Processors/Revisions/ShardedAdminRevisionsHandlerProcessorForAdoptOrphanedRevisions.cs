using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal sealed class ShardedAdminRevisionsHandlerProcessorForAdoptOrphanedRevisions : ShardedAdminRevisionsHandlerProcessorForRevisionsOperation<AdoptOrphanedRevisionsOperation.Parameters, AdoptOrphanedRevisionsResult>
    {
        public ShardedAdminRevisionsHandlerProcessorForAdoptOrphanedRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, OperationType.AdoptOrphanedRevisions)
        {
        }

        public override string Description => $"Adopt orphaned revisions in database '{RequestHandler.DatabaseName}'.";

        protected override AdoptOrphanedRevisionsOperation.Parameters GetOperationParameters(BlittableJsonReaderObject json)
        {
            var parameters = JsonDeserializationServer.Parameters.AdoptOrphanedRevisionsConfigurationOperationParameters(json);
            parameters.Collections = parameters.Collections?.Length > 0 ? parameters.Collections : null;
            return parameters;
        }

        protected override RavenCommand<OperationIdResult> GetCommand(JsonOperationContext context, int shardNumber, AdoptOrphanedRevisionsOperation.Parameters parameters)
        {
            return new AdoptOrphanedRevisionsOperation.AdoptOrphanedRevisionsCommand(parameters, DocumentConventions.DefaultForServer);
        }
    }
}
