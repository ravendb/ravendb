using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class AddEmbeddingsGenerationOperation(EmbeddingsGenerationConfiguration configuration) : IMaintenanceOperation<AddEtlOperationResult>
{
    public RavenCommand<AddEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new AddEtlOperation<AiConnectionString>.AddEtlCommand(conventions, configuration);
    }
}
