using System.Linq;
using System;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddGenAiCommand : AddEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public AddGenAiCommand()
    {
        // for deserialization
    }

    public AddGenAiCommand(GenAiConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
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
            throw new RachisApplyException("Failed to generate GenAI task identifier", e);
        }

        Validate(record);

        Add(ref record.GenAis, record, etag);
    }

    private void Validate(DatabaseRecord databaseRecord)
    {
        if (databaseRecord == null)
            throw new RachisApplyException("Failed to get database record, but it is required for further validation");

        if (string.IsNullOrWhiteSpace(Configuration.Identifier))
            throw new RachisApplyException("Integration task identifier must be set, but it is not");

        if (EmbeddingsGenerationConfiguration.ValidateIdentifier(Configuration.Identifier, out var errors) == false)
            throw new RachisApplyException($"Invalid identifier format. Validation errors:{Environment.NewLine} - {string.Join($"{Environment.NewLine} - ", errors)}");

        var isUpdate = databaseRecord.GenAis.Any(x => x.Name == Configuration.Name);

        var identifierConflicts = databaseRecord?.GenAis
            .Where(x => x.Identifier == Configuration.Identifier && x.Name != Configuration.Name)
            .ToArray();

        if (identifierConflicts.Length > 0)
            throw new RachisApplyException(
                $"Can't {(isUpdate ? "update" : "create")} GenAI task: '{Configuration.Name}'. " +
                $"The identifier '{Configuration.Identifier}' is already used by " +
                $"Gen AI task{(identifierConflicts.Length > 1 ? "s" : "")} " +
                $"'{string.Join("', '", identifierConflicts.Select(x => x.Name))}'");
    }
}
