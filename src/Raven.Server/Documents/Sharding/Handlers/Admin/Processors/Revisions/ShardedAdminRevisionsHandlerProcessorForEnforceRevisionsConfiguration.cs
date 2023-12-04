using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
    internal sealed class ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : ShardedAdminRevisionsHandlerProcessorForRevisionsOperation<EnforceRevisionsConfigurationOperation.Parameters>
    {
        public ShardedAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, OperationType.EnforceRevisionConfiguration)
        {
            Description = $"Enforce revision configuration in database '{RequestHandler.DatabaseName}'.";
        }

        protected override EnforceRevisionsConfigurationOperation.Parameters GetOperationParameters(BlittableJsonReaderObject json)
        {
            var parameters = JsonDeserializationServer.Parameters.EnforceRevisionsConfigurationOperationParameters(json);
            parameters.Collections = parameters.Collections?.Length > 0 ? parameters.Collections : null;
            return parameters;
        }

        protected override RavenCommand<OperationIdResult> GetCommand(JsonOperationContext context, int shardNumber, EnforceRevisionsConfigurationOperation.Parameters parameters)
        {
            return new EnforceRevisionsConfigurationOperation.EnforceRevisionsConfigurationCommand(parameters, DocumentConventions.DefaultForServer);
        }
    }
}
