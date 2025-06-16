using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class UpdateGenAiCommand : UpdateEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public string InitialChangeVector;

    public UpdateGenAiCommand()
    {
        // for deserialization
    }

    public UpdateGenAiCommand(long taskId, GenAiConfiguration configuration, string databaseName, string changeVector, string uniqueRequestId) : base(taskId, configuration, EtlType.GenAi, databaseName, uniqueRequestId)
    {
        InitialChangeVector = changeVector;
    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        InClusterValidation(record);

        new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.GenAi, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        new AddGenAiCommand(Configuration, DatabaseName, null, null).UpdateDatabaseRecord(record, etag);
    }

    public BlittableJsonReaderObject HandleChangeVectorInitialState(JsonOperationContext context, ServerStore serverStore, out string type)
    {
        type = string.Empty;
        if (InitialChangeVector == nameof(Constants.Documents.GenAiChangeVectorSpecialStates.DoNotChange))
            return null;

        UpdateValueForDatabaseCommand command;
        if (InitialChangeVector == nameof(Constants.Documents.GenAiChangeVectorSpecialStates.BeginningOfTime))
        {
            command = new RemoveEtlProcessStateCommand(DatabaseName, Configuration.Name, Configuration.Transforms[0].Name, UniqueRequestId);
            type = nameof(RemoveEtlProcessStateCommand);
        }
        else
        {
            type = nameof(UpdateEtlProcessStateCommand);

            var database = serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(DatabaseName).GetAwaiter().GetResult();
            long etag;
            if (InitialChangeVector == nameof(Constants.Documents.GenAiChangeVectorSpecialStates.LastDocument))
            {
                (etag, InitialChangeVector) = database.ReadLastEtagAndChangeVector();
            }
            else
            {
                etag = ChangeVectorUtils.GetEtagById(InitialChangeVector, database.DbBase64Id);
            }

            command = new UpdateEtlProcessStateCommand(DatabaseName, Configuration.Name, Configuration.Transforms[0].Name, etag, InitialChangeVector, serverStore.NodeTag,
                serverStore.LicenseManager.HasHighlyAvailableTasks(), database.DbBase64Id, RaftIdGenerator.NewId(), skippedTimeSeriesDocs: null, lastBatchTime: null);
        }

        return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(command, context);
    }

    private void InClusterValidation(DatabaseRecord record)
    {
        //TODO

        //try
        //{
        //    if (record == null)
        //        throw new RachisApplyException("Failed to get database record, but it is required for further validation");

        //    var oldConfig = record.EmbeddingsGenerations.FirstOrDefault(x => x.Name == Configuration.Name);
        //    if (oldConfig == null)
        //        return;

        //    if (oldConfig.AiConnectorType != Configuration.AiConnectorType)
        //    {
        //        throw new RachisApplyException(
        //            $"Cannot update Embeddings Generation task '{Configuration.Name}' because you are trying to change its connector type from '{oldConfig.AiConnectorType}' to '{Configuration.AiConnectorType}'. " +
        //            $"Changing the AI connector type requires recreating the embeddings to maintain data consistency. " +
        //            $"To proceed with these changes:{Environment.NewLine}" +
        //            $"1. Delete the existing Embeddings Generation task{Environment.NewLine}" +
        //            $"2. Create a new Embeddings Generation task with your desired connector type{Environment.NewLine}" +
        //            "This will ensure all documents are processed with consistent settings and maintain data integrity.");
        //    }

        //    if (oldConfig.ConnectionStringName != Configuration.ConnectionStringName)
        //    {
        //        var oldConnectionStringConfig = oldConfig.Connection;

        //        if (oldConnectionStringConfig == null && record.AiConnectionStrings.TryGetValue(oldConfig.ConnectionStringName, out oldConnectionStringConfig) == false)
        //        {
        //            throw new RachisApplyException($"Could not find AI connection string named '{oldConfig.ConnectionStringName}' in the database record");
        //        }

        //        var newConnectionStringConfig = Configuration.Connection;

        //        if (newConnectionStringConfig == null && record.AiConnectionStrings.TryGetValue(Configuration.ConnectionStringName, out newConnectionStringConfig) == false)
        //        {
        //            throw new RachisApplyException($"Could not find AI connection string named '{Configuration.ConnectionStringName}' in the database record");
        //        }

        //        var differences = oldConnectionStringConfig.Compare(newConnectionStringConfig);
        //        if (differences.HasFlag(AiSettingsCompareDifferences.RequiresEmbeddingsRegeneration))
        //        {
        //            throw new RachisApplyException(
        //                $"Cannot update Embeddings Generation task '{Configuration.Name}' because it contains critical changes ({differences}) in the connection settings that would affect the structure or creation process of embeddings. " +
        //                $"Changes to parameters like model selection, tokenization settings, embedding dimensions, or normalization options require recreating all embeddings to maintain consistency. " +
        //                $"To proceed with these changes:{Environment.NewLine}" +
        //                $"1. Delete the existing Embeddings Generation task{Environment.NewLine}" +
        //                $"2. Create a new Embeddings Generation task with your desired settings{Environment.NewLine}" +
        //                "This will ensure all documents are processed with consistent settings and maintain data integrity. " +
        //                "Note: While you can update non-critical settings like API keys or endpoints without recreating the task, your current changes include critical modifications that affect the embedding process.");
        //        }
        //    }
        //}
        //catch (Exception e) when (ClusterStateMachine.ExpectedException(e))
        //{
        //    throw;
        //}
        //catch (Exception e)
        //{
        //    throw new RachisApplyException("Failed to validate AI Integration configuration", e);
        //}
    }

    public override DynamicJsonValue ToJson(JsonOperationContext context)
    {
        var json = base.ToJson(context);
        json[nameof(InitialChangeVector)] = InitialChangeVector;

        return json;
    }
}
