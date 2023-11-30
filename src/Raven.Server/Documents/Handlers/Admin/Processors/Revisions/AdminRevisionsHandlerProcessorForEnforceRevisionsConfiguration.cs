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

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
internal sealed class AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AdminRevisionsHandlerProcessorForRevisionsOperation<EnforceRevisionsConfigurationOperation.Parameters>
{
    public AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, OperationType.EnforceRevisionConfiguration)
    {
        Description = $"Enforce revision configuration in database '{RequestHandler.DatabaseName}'.";
    }

    protected override EnforceRevisionsConfigurationOperation.Parameters GetOperationParameters(BlittableJsonReaderObject json)
    {
        return JsonDeserializationServer.Parameters.EnforceRevisionsConfigurationOperationParameters(json);
    }

    protected override Task<IOperationResult> ExecuteOperation(Action<IOperationProgress> onProgress, EnforceRevisionsConfigurationOperation.Parameters parameters, OperationCancelToken token)
    {
        return RequestHandler.Database.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(onProgress, parameters.IncludeForceCreated, 
            parameters.Collections.ToHashSet(StringComparer.OrdinalIgnoreCase), token);
    }
}

