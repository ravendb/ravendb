using System;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class TasksManagementTests : RavenTestBase
{
    protected static readonly ChunkingOptions DefaultChunkingOptions = new() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 };
    
    public TasksManagementTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void CanDeleteTask()
    {
        using var store = GetDocumentStore();

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "PostContent", ChunkingOptions = DefaultChunkingOptions }, new EmbeddingPathConfiguration(){ Path = "Comments", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Posts",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
        };

        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, EmbeddedSettings = new EmbeddedSettings() };

        var putAiConnectionStringResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putAiConnectionStringResult.RaftCommandIndex);

        var addAiIntegrationTaskResult = store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));
        Assert.NotNull(addAiIntegrationTaskResult.RaftCommandIndex);
        Assert.NotNull(addAiIntegrationTaskResult.TaskId);

        store.Maintenance.Send(new DeleteOngoingTaskOperation(addAiIntegrationTaskResult.TaskId, OngoingTaskType.EmbeddingsGeneration));

        var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(addAiIntegrationTaskResult.TaskId, OngoingTaskType.EmbeddingsGeneration));

        Assert.Null(ongoingTask);
    }

    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public void CanUpdateTask()
    {
        using var store = GetDocumentStore();

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "PostContent", ChunkingOptions = DefaultChunkingOptions }, new EmbeddingPathConfiguration() { Path = "Comments", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Posts",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
        };

        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, EmbeddedSettings = new EmbeddedSettings() };

        var putAiConnectionStringResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putAiConnectionStringResult.RaftCommandIndex);

        var addAiIntegrationTaskResult = store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));
        Assert.NotNull(addAiIntegrationTaskResult.RaftCommandIndex);
        Assert.NotNull(addAiIntegrationTaskResult.TaskId);

        configuration.Disabled = true;
        configuration.Identifier = addAiIntegrationTaskResult.Identifier;

        var update = store.Maintenance.Send(new UpdateEmbeddingsGenerationOperation(addAiIntegrationTaskResult.TaskId, configuration));

        var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(update.TaskId, OngoingTaskType.EmbeddingsGeneration));

        Assert.Equal(OngoingTaskState.Disabled, ongoingTask.TaskState);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void EmbeddingsGenerationConfiguration_WithoutChunkingOptions_ShouldProvideMeaningfulError()
    {
        using var store = GetDocumentStore();

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = "ai-task-testing",
            ConnectionStringName = "ai-service-connection",
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration { Path = "PostContent" }],
            Collection = "Posts",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
        };

        var connectionString = new AiConnectionString { Name = configuration.ConnectionStringName, EmbeddedSettings = new EmbeddedSettings() };

        var putAiConnectionStringResult = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        Assert.NotNull(putAiConnectionStringResult.RaftCommandIndex);

        var exception = Record.Exception(() => store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration)));
        Assert.NotNull(exception);
        Assert.IsType<RavenException>(exception);
        Assert.True(exception.Message.Contains("Path 'PostContent': ChunkingOptions must be provided"),
            $"The exception must include a description of the problem regarding the missing ChunkingOptions, but it didn't:{Environment.NewLine}`{exception.Message}`");
    }
}
