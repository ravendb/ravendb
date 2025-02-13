using System;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class UpdateAiIntegrationCommand : UpdateEtlCommand<AiIntegrationConfiguration, AiConnectionString>
{
    public UpdateAiIntegrationCommand()
    {
        // for deserialization
    }

    public UpdateAiIntegrationCommand(long taskId, AiIntegrationConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Ai, databaseName, uniqueRequestId)
    {

    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        InClusterValidation(record);

        new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.AiIntegration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        new AddAiIntegrationCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
    }

    private void InClusterValidation(DatabaseRecord record)
    {
        try
        {
            if (record == null)
                throw new RachisApplyException("Failed to get database record, but it is required for further validation");

            var oldConfig = record.AiIntegrations.FirstOrDefault(x => x.Name == Configuration.Name);
            if (oldConfig == null)
                return;

            if (oldConfig.AiConnectorType != Configuration.AiConnectorType)
            {
                throw new RachisApplyException(
                    $"Cannot update AI Integration task '{Configuration.Name}' because you are trying to change its connector type from '{oldConfig.AiConnectorType}' to '{Configuration.AiConnectorType}'. " +
                    $"Changing the AI connector type requires recreating the embeddings to maintain data consistency. " +
                    $"To proceed with these changes:{Environment.NewLine}" +
                    $"1. Delete the existing Integration task{Environment.NewLine}" +
                    $"2. Create a new Integration task with your desired connector type{Environment.NewLine}" +
                    "This will ensure all documents are processed with consistent settings and maintain data integrity.");
            }

            var differences = oldConfig.Connection.Compare(Configuration.Connection);
            if (differences.HasFlag(AiSettingsCompareDifferences.RequiresEmbeddingsRegeneration))
            {
                throw new RachisApplyException(
                    $"Cannot update AI Integration task '{Configuration.Name}' because it contains critical changes in the connection settings that would affect the structure or creation process of embeddings. " +
                    $"Changes to parameters like model selection, tokenization settings, embedding dimensions, or normalization options require recreating all embeddings to maintain consistency. " +
                    $"To proceed with these changes:{Environment.NewLine}" +
                    $"1. Delete the existing Integration task{Environment.NewLine}" +
                    $"2. Create a new Integration task with your desired settings{Environment.NewLine}" +
                    "This will ensure all documents are processed with consistent settings and maintain data integrity. " +
                    "Note: While you can update non-critical settings like API keys or endpoints without recreating the task, your current changes include critical modifications that affect the embedding process.");
            }
        }
        catch (Exception e) when (ClusterStateMachine.ExpectedException(e))
        {
            throw;
        }
        catch (Exception e)
        {
            throw new RachisApplyException("Failed to validate AI ETL configuration", e);
        }
    }
}
