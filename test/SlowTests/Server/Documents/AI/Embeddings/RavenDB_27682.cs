using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RavenDB_27682(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task TaskWhoseWorkerFailsToStart_DoesNotBlockOtherTasks()
    {
        using var store = GetDocumentStore();

        PutEmbeddedConnectionString(store, "broken connection", embeddingsMaxConcurrentBatches: 0);
        var (brokenTask, _) = AddTask(store, "brokenTask", "broken connection");
        var brokenTaskId = new EmbeddingsGenerationTaskIdentifier(brokenTask.Identifier);
        var (healthyTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "healthyTask", connectionStringName: "healthy connection");
        var healthyTaskId = new EmbeddingsGenerationTaskIdentifier(healthyTask.Identifier);

        var (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, healthyTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);

        var database = await GetDatabase(store.Database);
        Assert.False(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(brokenTaskId));
        Assert.False(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(brokenTaskId));

        using var generator = new EmbeddingsGenerator(database, database.Loggers.GetLogger<EmbeddingsGenerator>(),
            database.DatabaseShutdown, EmbeddingsGenerator.Mode.Etl);
        generator.Start();

        Assert.True(await WaitForValueAsync(() => generator.EmbeddingTaskExists(healthyTaskId), true));
        Assert.False(generator.EmbeddingTaskExists(brokenTaskId));
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task TaskWhoseWorkerFailsToRestartAfterUpdate_IsUnregisteredAndDoesNotBlockOtherTasks()
    {
        using var store = GetDocumentStore();

        PutEmbeddedConnectionString(store, "breakable connection", embeddingsMaxConcurrentBatches: null);
        PutEmbeddedConnectionString(store, "healthy connection", embeddingsMaxConcurrentBatches: null);

        var (taskToBreak, taskToBreakAddResult) = AddTask(store, "taskToBreak", "breakable connection");
        var (healthyTask, _) = AddTask(store, "healthyTask", "healthy connection");
        var taskToBreakId = new EmbeddingsGenerationTaskIdentifier(taskToBreak.Identifier);
        var healthyTaskId = new EmbeddingsGenerationTaskIdentifier(healthyTask.Identifier);

        var (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, taskToBreak);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);
        (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, healthyTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);

        PutEmbeddedConnectionString(store, "breakable connection", embeddingsMaxConcurrentBatches: 0);
        taskToBreak.EmbeddingsCacheExpiration = TimeSpan.FromDays(1);
        store.Maintenance.Send(new UpdateEmbeddingsGenerationOperation(taskToBreakAddResult.TaskId, taskToBreak));

        var database = await GetDatabase(store.Database);
        Assert.False(await WaitForValueAsync(() => database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(taskToBreakId), false));
        Assert.False(await WaitForValueAsync(() => database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(taskToBreakId), false));
        Assert.True(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(healthyTaskId));
        Assert.True(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(healthyTaskId));

        var (lateTask, _) = AddTask(store, "lateTask", "healthy connection");

        (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, lateTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);
        Assert.True(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(healthyTaskId));
        Assert.True(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(healthyTaskId));
        Assert.False(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(taskToBreakId));
        Assert.False(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(taskToBreakId));
    }

    private static void PutEmbeddedConnectionString(IDocumentStore store, string name, int? embeddingsMaxConcurrentBatches)
    {
        var connectionString = new AiConnectionString
        {
            Name = name,
            ModelType = AiModelType.TextEmbeddings,
            EmbeddedSettings = new EmbeddedSettings { EmbeddingsMaxConcurrentBatches = embeddingsMaxConcurrentBatches }
        };
        connectionString.Identifier = connectionString.GenerateIdentifier();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));
    }

    private static (EmbeddingsGenerationConfiguration Configuration, AddEmbeddingsGenerationOperationResult AddResult) AddTask(IDocumentStore store, string name, string connectionStringName)
    {
        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration { Path = "Name", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Dtos",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
        };
        configuration.Identifier = configuration.GenerateIdentifier();
        var addResult = store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));

        return (configuration, addResult);
    }
}
