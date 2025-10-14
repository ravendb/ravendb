using System;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Replication;
using Raven.Server.Logging;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class UpdateGenAiCommand : UpdateEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public string ChangeVectorForStartingPoint;

    [JsonDeserializationIgnore]
    public long Index;

    public UpdateGenAiCommand()
    {
        // for deserialization
    }

    public UpdateGenAiCommand(long taskId, GenAiConfiguration configuration, string databaseName, string changeVector, string uniqueRequestId) : base(taskId, configuration, EtlType.GenAi, databaseName, uniqueRequestId)
    {
        ChangeVectorForStartingPoint = changeVector;
    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        Index = etag;

        InClusterValidation(record);

        new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.GenAi, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        new AddGenAiCommand(Configuration, DatabaseName, ChangeVectorForStartingPoint, null).UpdateDatabaseRecord(record, etag);
    }

    public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, RavenAuditLogger clusterAuditLog) => 
        UpdateGenAiState(ctx, items, DatabaseName, Configuration, StartingPointChangeVector.From(ChangeVectorForStartingPoint), Index);

    public static void UpdateGenAiState(ClusterOperationContext ctx, Table items, string database, GenAiConfiguration configuration, StartingPointChangeVector changeVectorForStartingPoint, long index)
    {
        if (changeVectorForStartingPoint?.Value == null || 
            changeVectorForStartingPoint == StartingPointChangeVector.DoNotChange)
            return;

        if (changeVectorForStartingPoint == StartingPointChangeVector.LastDocument)
            throw new RachisInvalidOperationException($"You can't pass '{StartingPointChangeVector.LastDocument}' here directly");

        var itemKey = EtlProcessState.GenerateItemName(database, configuration.Name, configuration.Transforms[0].Name);
        
        using var _ = Slice.From(ctx.Allocator, itemKey, out Slice valueName);
        using var __ = Slice.From(ctx.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered);
        var stateBlittable = ClusterStateMachine.ReadInternal(ctx, out var _, valueNameLowered);

        var etl = stateBlittable != null ? JsonDeserializationClient.EtlProcessState(stateBlittable) : new EtlProcessState
        {
            ConfigurationName = configuration.Name,
            TransformationName = configuration.Transforms[0].Name
        };

        if (changeVectorForStartingPoint == StartingPointChangeVector.BeginningOfTime)
        {
            etl.ChangeVector = null;
            etl.LastProcessedEtagPerDbId.Clear();
        }
        else
        {
            etl.LastProcessedEtagPerDbId.Clear();
            foreach (var entry in changeVectorForStartingPoint.Value.ToChangeVectorList() ?? [])
            {
                etl.LastProcessedEtagPerDbId.Add(entry.DbId, entry.Etag);
            }
            etl.ChangeVector = changeVectorForStartingPoint.Value;
        }

        using (var updatedValue = ctx.ReadObject(etl.ToJson(), "update-genai-state"))
        {
            ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, updatedValue);
        }
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
        json[nameof(ChangeVectorForStartingPoint)] = ChangeVectorForStartingPoint;

        return json;
    }

    public override bool Disabled => Configuration.Disabled;
}
