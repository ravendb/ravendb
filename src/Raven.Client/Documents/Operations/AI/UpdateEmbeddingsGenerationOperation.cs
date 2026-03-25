using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Updates an existing embeddings generation ETL task definition.
/// Use this operation to modify the configuration of an existing task and optionally reset its transformation.
/// </summary>
public class UpdateEmbeddingsGenerationOperation(long taskId, EmbeddingsGenerationConfiguration configuration, bool reset = false) : IMaintenanceOperation<UpdateEtlOperationResult>
{
    /// <summary>
    /// Creates the command that will be executed by the maintenance executor.
    /// </summary>
    public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        List<string> transformationsToReset = null;

        if (reset)
            transformationsToReset = [configuration.TransformationName];
        
        return new UpdateEtlOperation<AiConnectionString>.UpdateEtlCommand(conventions, taskId, configuration, transformationsToReset);
    }
}
