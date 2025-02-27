using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class TasksManagementTests : RavenTestBase
{
    protected static readonly ChunkingOptions DefaultChunkingOptions = new() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 };
    
    public TasksManagementTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CanDeleteTask()
    {
        using var store = GetDocumentStore();

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "PostContent" }, new EmbeddingPathConfiguration(){ Path = "Comments", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Posts",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
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

    [RavenFact(RavenTestCategory.Ai)]
    public void CanUpdateTask()
    {
        using var store = GetDocumentStore();

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "PostContent" }, new EmbeddingPathConfiguration() { Path = "Comments", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Posts",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
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
