using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class UpdateEmbeddingsGenerationOperation(long taskId, EmbeddingsGenerationConfiguration configuration, List<string> transformationsToReset = null) : IMaintenanceOperation<UpdateEtlOperationResult>
{
    public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new UpdateEtlOperation<AiConnectionString>.UpdateEtlCommand(conventions, taskId, configuration, transformationsToReset);
    }
}
