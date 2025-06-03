using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands;

public class EditSchemaValidationConfigurationCommand : UpdateDatabaseCommand
{
    public SchemaValidationConfiguration Configuration;

    public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
    {
        databaseRecord.SchemaValidation = Configuration;
    }

    public EditSchemaValidationConfigurationCommand()
    {
        // for deserialization
    }

    public EditSchemaValidationConfigurationCommand(SchemaValidationConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
        Configuration = configuration;
    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        record.SchemaValidation = Configuration;
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(Configuration)] = Configuration.ToJson();
    }
}
