using System.Linq;
using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.Logging;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddGenAiCommand : AddEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public string ChangeVectorForStartingPoint;

    [JsonDeserializationIgnore]
    public long Index;

    public AddGenAiCommand()
    {
        // for deserialization
    }

    public AddGenAiCommand(GenAiConfiguration configuration, string databaseName, string changeVector, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
    {
        ChangeVectorForStartingPoint = changeVector;
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

        Index = etag;

        Validate(record);

        Add(ref record.GenAis, record, etag);
    }

    private void Validate(DatabaseRecord databaseRecord)
    {
        if (databaseRecord == null)
            throw new RachisApplyException("Failed to get database record, but it is required for further validation");

        if (string.IsNullOrWhiteSpace(Configuration.Identifier))
            throw new RachisApplyException("Integration task identifier must be set, but it is not");

        if (AiTaskIdentifierHelper.ValidateIdentifier(Configuration.Identifier, out var errors) == false)
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

    public override DynamicJsonValue ToJson(JsonOperationContext context)
    {
        var json = base.ToJson(context);
        json[nameof(ChangeVectorForStartingPoint)] = ChangeVectorForStartingPoint;

        return json;
    }

    public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, RavenAuditLogger clusterAuditLog)
        => UpdateGenAiCommand.UpdateGenAiState(ctx, items, DatabaseName, Configuration, StartingPointChangeVector.From(ChangeVectorForStartingPoint), Index);
}
