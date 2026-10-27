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

        var brokenTaskId = AddTaskWithZeroMaxConcurrentBatches(store);
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

    private static EmbeddingsGenerationTaskIdentifier AddTaskWithZeroMaxConcurrentBatches(IDocumentStore store)
    {
        var connectionString = new AiConnectionString
        {
            Name = "broken connection",
            ModelType = AiModelType.TextEmbeddings,
            EmbeddedSettings = new EmbeddedSettings { EmbeddingsMaxConcurrentBatches = 0 }
        };
        connectionString.Identifier = connectionString.GenerateIdentifier();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connectionString));

        var configuration = new EmbeddingsGenerationConfiguration
        {
            Name = "brokenTask",
            ConnectionStringName = connectionString.Name,
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration { Path = "Name", ChunkingOptions = DefaultChunkingOptions }],
            Collection = "Dtos",
            ChunkingOptionsForQuerying = DefaultChunkingOptions
        };
        configuration.Identifier = configuration.GenerateIdentifier();
        store.Maintenance.Send(new AddEmbeddingsGenerationOperation(configuration));

        return new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);
    }
}
