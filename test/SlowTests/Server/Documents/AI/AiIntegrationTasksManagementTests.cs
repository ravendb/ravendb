using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI;

public class AiIntegrationTasksManagementTests : RavenTestBase
{
    public AiIntegrationTasksManagementTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
    public void CanDeleteAiIntegrationTask()
    {
        using var store = GetDocumentStore();

        var configuration = new AiIntegrationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPaths = ["PostContent", "Comments"], 
            Collection = "Posts",
        };

        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, OnnxSettings = new OnnxSettings() };

        var putAiConnectionStringResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putAiConnectionStringResult.RaftCommandIndex);

        var addAiIntegrationTaskResult = store.Maintenance.Send(new AddAiIntegrationOperation(configuration));
        Assert.NotNull(addAiIntegrationTaskResult.RaftCommandIndex);
        Assert.NotNull(addAiIntegrationTaskResult.TaskId);

        store.Maintenance.Send(new DeleteOngoingTaskOperation(addAiIntegrationTaskResult.TaskId, OngoingTaskType.AiIntegration));

        var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(addAiIntegrationTaskResult.TaskId, OngoingTaskType.AiIntegration));

        Assert.Null(ongoingTask);
    }

    [RavenFact(RavenTestCategory.AiIntegration)]
    public void CanUpdateAiIntegrationTask()
    {
        using var store = GetDocumentStore();

        var configuration = new AiIntegrationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPaths = ["PostContent", "Comments"],
            Collection = "Posts",
        };

        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, OnnxSettings = new OnnxSettings() };

        var putAiConnectionStringResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putAiConnectionStringResult.RaftCommandIndex);

        var addAiIntegrationTaskResult = store.Maintenance.Send(new AddAiIntegrationOperation(configuration));
        Assert.NotNull(addAiIntegrationTaskResult.RaftCommandIndex);
        Assert.NotNull(addAiIntegrationTaskResult.TaskId);

        configuration.Disabled = true;

        var update = store.Maintenance.Send(new UpdateAiIntegrationOperation(addAiIntegrationTaskResult.TaskId, configuration));

        var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(update.TaskId, OngoingTaskType.AiIntegration));

        Assert.Equal(OngoingTaskState.Disabled, ongoingTask.TaskState);
    }
}
