using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RavenDB_27682(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task TaskWhoseWorkerFailsToStart_DoesNotBlockOtherTasks()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);
        FailWorkerCreationFor(database.EmbeddingsGeneratorQueries, "brokenTask");
        FailWorkerCreationFor(database.EmbeddingsGeneratorEtl, "brokenTask");

        var (brokenTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "brokenTask", connectionStringName: "broken connection");
        var brokenTaskId = new EmbeddingsGenerationTaskIdentifier(brokenTask.Identifier);
        var (healthyTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "healthyTask", connectionStringName: "healthy connection");
        var healthyTaskId = new EmbeddingsGenerationTaskIdentifier(healthyTask.Identifier);

        var (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, healthyTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);

        Assert.False(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(brokenTaskId));
        Assert.False(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(brokenTaskId));

        using var generator = new EmbeddingsGenerator(database, database.Loggers.GetLogger<EmbeddingsGenerator>(),
            database.DatabaseShutdown, EmbeddingsGenerator.Mode.Etl);
        FailWorkerCreationFor(generator, "brokenTask");
        generator.Start();

        Assert.True(await WaitForValueAsync(() => generator.EmbeddingTaskExists(healthyTaskId), true));
        Assert.False(generator.EmbeddingTaskExists(brokenTaskId));
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task TaskWhoseWorkerFailsToStart_SurfacesTheFailureToEtlAndQueries()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);
        FailWorkerCreationFor(database.EmbeddingsGeneratorQueries, "brokenTask");
        FailWorkerCreationFor(database.EmbeddingsGeneratorEtl, "brokenTask");

        var (brokenTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "brokenTask", connectionStringName: "broken connection");
        var brokenTaskId = new EmbeddingsGenerationTaskIdentifier(brokenTask.Identifier);
        var (healthyTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "healthyTask", connectionStringName: "healthy connection");

        var (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, healthyTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);

        var etlException = Assert.Throws<InvalidOperationException>(() => database.EmbeddingsGeneratorEtl.BatchFor(brokenTaskId));
        Assert.Contains(brokenTask.Identifier, etlException.Message);
        Assert.Equal(WorkerCreationFailureMessage, etlException.InnerException?.Message);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var queryException = Assert.Throws<InvalidOperationException>(() => { database.EmbeddingsGeneratorQueries.GetEmbeddingsForQuery(context, brokenTaskId, "text"); });
            Assert.Contains(brokenTask.Identifier, queryException.Message);
            Assert.Equal(WorkerCreationFailureMessage, queryException.InnerException?.Message);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task TaskWhoseWorkerFailsToRestartAfterUpdate_IsUnregisteredAndDoesNotBlockOtherTasks()
    {
        using var store = GetDocumentStore();
        var database = await GetDatabase(store.Database);
        var breakTaskToBreak = false;
        FailWorkerCreationFor(database.EmbeddingsGeneratorQueries, "taskToBreak", () => breakTaskToBreak);
        FailWorkerCreationFor(database.EmbeddingsGeneratorEtl, "taskToBreak", () => breakTaskToBreak);

        var (taskToBreak, taskToBreakAddResult) = AddTask(store, "taskToBreak", "breakable connection");
        var (healthyTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "healthyTask", connectionStringName: "healthy connection");
        var taskToBreakId = new EmbeddingsGenerationTaskIdentifier(taskToBreak.Identifier);
        var healthyTaskId = new EmbeddingsGenerationTaskIdentifier(healthyTask.Identifier);

        var (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, taskToBreak);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);
        (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, healthyTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);

        breakTaskToBreak = true;
        taskToBreak.EmbeddingsCacheExpiration = TimeSpan.FromDays(1);
        store.Maintenance.Send(new UpdateEmbeddingsGenerationOperation(taskToBreakAddResult.TaskId, taskToBreak));

        Assert.False(await WaitForValueAsync(() => database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(taskToBreakId), false));
        Assert.False(await WaitForValueAsync(() => database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(taskToBreakId), false));
        Assert.True(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(healthyTaskId));
        Assert.True(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(healthyTaskId));

        var (lateTask, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "lateTask", connectionStringName: "healthy connection");

        (queriesWorkerRegistered, etlWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, lateTask);
        Assert.True(queriesWorkerRegistered);
        Assert.True(etlWorkerRegistered);
        Assert.True(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(healthyTaskId));
        Assert.True(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(healthyTaskId));
        Assert.False(database.EmbeddingsGeneratorQueries.EmbeddingTaskExists(taskToBreakId));
        Assert.False(database.EmbeddingsGeneratorEtl.EmbeddingTaskExists(taskToBreakId));
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Etl)]
    [InlineData(0)]
    [InlineData(-1)]
    public void PutConnectionString_RejectsNonPositiveEmbeddingsMaxConcurrentBatches(int embeddingsMaxConcurrentBatches)
    {
        using var store = GetDocumentStore();

        var connectionString = new AiConnectionString
        {
            Name = "invalid connection",
            ModelType = AiModelType.TextEmbeddings,
            EmbeddedSettings = new EmbeddedSettings { EmbeddingsMaxConcurrentBatches = embeddingsMaxConcurrentBatches }
        };
        connectionString.Identifier = connectionString.GenerateIdentifier();

        var exception = Assert.Throws<BadRequestException>(() => store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString)));
        Assert.Contains($"Value of `{nameof(AbstractAiSettings.EmbeddingsMaxConcurrentBatches)}` field must be a positive number.", exception.Message);
    }

    private static (EmbeddingsGenerationConfiguration Configuration, AddEmbeddingsGenerationOperationResult AddResult) AddTask(IDocumentStore store, string name, string connectionStringName)
    {
        var connectionString = new AiConnectionString { Name = connectionStringName, ModelType = AiModelType.TextEmbeddings, EmbeddedSettings = new EmbeddedSettings() };
        connectionString.Identifier = connectionString.GenerateIdentifier();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));

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

    private const string WorkerCreationFailureMessage = "Worker creation failed on purpose";

    private static void FailWorkerCreationFor(EmbeddingsGenerator generator, string taskName, Func<bool> shouldFail = null)
    {
        generator.ForTestingPurposesOnly().BeforeCreateAiWorker = configuration =>
        {
            if (configuration.Name == taskName && (shouldFail?.Invoke() ?? true))
                throw new InvalidOperationException(WorkerCreationFailureMessage);
        };
    }
}
