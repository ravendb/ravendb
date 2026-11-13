using Raven.Client;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
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
        KeepLastModifiedTimeOfUnchangedDefinitions(record.SchemaValidation);
        record.SchemaValidation = Configuration;

        if (Configuration == null || Configuration.ValidatorsPerCollection == null)
            return;

        foreach (var keyValue in Configuration.ValidatorsPerCollection)
        {
            if (keyValue.Key.StartsWith('@') && keyValue.Key != Constants.Documents.Collections.EmptyCollection)
            {
                throw new RachisApplyException($"Schema validation cannot be defined for system collections other than '{Constants.Documents.Collections.EmptyCollection}'. Invalid collection name: '{keyValue.Key}'.");
            }
        }

        if (record.Indexes == null)
            return;

        foreach (var index in record.Indexes.Values)
        {
            if (index.Reduce == null || index.OutputReduceToCollection == null)
                continue;

            if (Configuration.ValidatorsPerCollection.TryGetValue(index.OutputReduceToCollection, out _))
            {
                throw new RachisApplyException($"Cannot have an index that outputs to collection '{index.OutputReduceToCollection}' which has an active schema validation defined.");
            }

            if (index.PatternReferencesCollectionName != null &&
                Configuration.ValidatorsPerCollection.TryGetValue(index.PatternReferencesCollectionName, out _))
            {
                throw new RachisApplyException($"Cannot have an index that uses the pattern for outputs the pattern to collection '{index.PatternReferencesCollectionName}' which has an active schema validation defined.");
            }
        }
    }

    private void KeepLastModifiedTimeOfUnchangedDefinitions(SchemaValidationConfiguration current)
    {
        // done against the record the command is applied to, so every node stores the same value
        if (Configuration?.ValidatorsPerCollection == null || current?.ValidatorsPerCollection == null)
            return;

        foreach ((string collection, SchemaDefinition definition) in Configuration.ValidatorsPerCollection)
        {
            if (current.ValidatorsPerCollection.TryGetValue(collection, out SchemaDefinition existing) == false)
                continue;

            if (definition.IsSameAs(existing))
                definition.LastModifiedTime = existing.LastModifiedTime;
        }
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(Configuration)] = Configuration.ToJson();
    }
}
