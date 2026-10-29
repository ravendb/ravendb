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

    [RavenFact(RavenTestCategory.Ai)]
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

    [RavenFact(RavenTestCategory.Ai)]
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

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void UpdateInUseConnectionString_WithEmbeddingStructureChange_ShouldThrow(bool changeModel, bool changeDimensions)
    {
        using var store = GetDocumentStore();

        var connectionString = CreateOpenAiConnectionString("ai-service-connection", "text-embedding-3-small", 512);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        store.Maintenance.Send(new AddEmbeddingsGenerationOperation(CreateConfiguration("ai-task-testing", connectionString.Name)));

        var updated = CreateOpenAiConnectionString(connectionString.Name,
            changeModel ? "text-embedding-3-large" : "text-embedding-3-small",
            changeDimensions ? 1024 : 512);

        var exception = Assert.Throws<RavenException>(() => store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(updated)));
        Assert.Contains("would affect the structure or creation process of embeddings", exception.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void UpdateInUseConnectionString_WithApiKeyChangeOnly_ShouldWork()
    {
        using var store = GetDocumentStore();

        var connectionString = CreateOpenAiConnectionString("ai-service-connection", "text-embedding-3-small", 512);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
        store.Maintenance.Send(new AddEmbeddingsGenerationOperation(CreateConfiguration("ai-task-testing", connectionString.Name)));

        var updated = CreateOpenAiConnectionString(connectionString.Name, "text-embedding-3-small", 512, apiKey: "rotated-key");
        var result = store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(updated));
        Assert.NotNull(result.RaftCommandIndex);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public void UpdateTask_SwitchingToConnectionStringWithEmbeddingStructureChange_ShouldThrow(bool changeModel, bool changeDimensions)
    {
        using var store = GetDocumentStore();

        var oldConnectionString = CreateOpenAiConnectionString("ai-service-connection-old", "text-embedding-3-small", 512);
        var newConnectionString = CreateOpenAiConnectionString("ai-service-connection-new",
            changeModel ? "text-embedding-3-large" : "text-embedding-3-small",
            changeDimensions ? 1024 : 512);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(oldConnectionString));
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(newConnectionString));

        var configuration = CreateConfiguration("ai-task-testing", oldConnectionString.Name);
        var addResult = store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));

        configuration.Identifier = addResult.Identifier;
        configuration.ConnectionStringName = newConnectionString.Name;

        var exception = Assert.Throws<RavenException>(() => store.Maintenance.Send(new UpdateEmbeddingsGenerationOperation(addResult.TaskId, configuration)));
        Assert.Contains("would affect the structure or creation process of embeddings", exception.Message);
    }

    private static AiConnectionString CreateOpenAiConnectionString(string name, string model, int dimensions, string apiKey = "test-key")
    {
        return new AiConnectionString
        {
            Name = name,
            Identifier = name,
            ModelType = AiModelType.TextEmbeddings,
            OpenAiSettings = new OpenAiSettings(apiKey, "https://api.openai.com/v1", model, dimensions: dimensions)
        };
    }

    private static EmbeddingsGenerationConfiguration CreateConfiguration(string name, string connectionStringName)
    {
        return new EmbeddingsGenerationConfiguration
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration { Path = "PostContent", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Posts",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
        };
    }
}
