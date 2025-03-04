using System;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddEmbeddingsGenerationCommand : AddEtlCommand<EmbeddingsGenerationConfiguration, AiConnectionString>
{
    public AddEmbeddingsGenerationCommand()
    {
        // for deserialization
    }

    public AddEmbeddingsGenerationCommand(EmbeddingsGenerationConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
    {

    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        try
        {
            if (string.IsNullOrEmpty(Configuration.Identifier))
                Configuration.Identifier = Configuration.GenerateIdentifier();
        }
        catch (Exception e)
        {
            throw new RachisApplyException("Failed to generate Embeddings Generation task identifier", e);
        }

        InClusterValidation(record);

        Add(ref record.EmbeddingsGenerations, record, etag);
    }

    private void InClusterValidation(DatabaseRecord databaseRecord)
    {
        try
        {
            if (databaseRecord == null)
                throw new RachisApplyException("Failed to get database record, but it is required for further validation");

            if (string.IsNullOrWhiteSpace(Configuration.Identifier))
                throw new RachisApplyException("Integration task identifier must be set, but it is not");

            if (Configuration.ValidateIdentifier(out var errors) == false)
                throw new RachisApplyException($"Invalid identifier format. Validation errors:{Environment.NewLine} - {string.Join($"{Environment.NewLine} - ", errors)}");

            var isUpdate = databaseRecord.EmbeddingsGenerations.Any(x => x.Name == Configuration.Name);
                
            var identifierConflicts = databaseRecord?.EmbeddingsGenerations
                .Where(x => x.Identifier == Configuration.Identifier && x.Name != Configuration.Name)
                .ToArray();

            if (identifierConflicts.Length > 0)
                throw new RachisApplyException(
                    $"Can't {(isUpdate ? "update" : "create")} AI Integration task: '{Configuration.Name}'. " +
                    $"The identifier '{Configuration.Identifier}' is already used by " +
                    $"AI task{(identifierConflicts.Length > 1 ? "s" : "")} " +
                    $"'{string.Join("', '", identifierConflicts.Select(x => x.Name))}'");
        }
        catch (Exception e) when (ClusterStateMachine.ExpectedException(e))
        {
            throw;
        }
        catch (Exception e)
        {
            throw new RachisApplyException("Failed to validate AI connection string", e);
        }
    }
}
