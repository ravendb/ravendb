using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25692(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DynamicQueryOnDisabledEmbeddingsGenerationTask(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            var taskInfo = store.Maintenance.Send(new GetOngoingTaskInfoOperation(configuration.Identifier, OngoingTaskType.EmbeddingsGeneration));

            var disableOp = new ToggleOngoingTaskStateOperation(taskInfo.TaskId, OngoingTaskType.EmbeddingsGeneration, disable: true);
            store.Maintenance.Send(disableOp);

            using (var session = store.OpenAsyncSession())
            {
                var ex = await Assert.ThrowsAsync<InvalidQueryException>(async () => await session.Query<Dto>().VectorSearch(x => x.WithText(d => d.TextualValue).UsingTask(configuration.Identifier), factory => factory.ByText("some text"), minimumSimilarity: 0.75f).ToListAsync());

                Assert.Contains($"Embeddings Generation task with '{configuration.Identifier}' identifier is disabled, and cannot be used for querying", ex.Message);
            }

            var indexNames = store.Maintenance.Send(new Raven.Client.Documents.Operations.Indexes.GetIndexNamesOperation(0, 10));
            Assert.Empty(indexNames);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task StaticIndexQueryOnDisabledEmbeddingsGenerationTask(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Dto { TextualValue = "apple" });
                session.SaveChanges();
            }

            var index = new TextualValueIndex();
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            var etlStatus = Etl.WaitForEtlToComplete(store);
            var (configuration, _) = AddEmbeddingsGenerationTask(store,
                embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

            Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);

            var taskInfo = store.Maintenance.Send(new GetOngoingTaskInfoOperation(configuration.Identifier, OngoingTaskType.EmbeddingsGeneration));

            var disableOp = new ToggleOngoingTaskStateOperation(taskInfo.TaskId, OngoingTaskType.EmbeddingsGeneration, disable: true);
            store.Maintenance.Send(disableOp);

            using (var session = store.OpenAsyncSession())
            {
                var ex = await Assert.ThrowsAsync<InvalidQueryException>(async () =>
                    await session.Query<Dto, TextualValueIndex>()
                        .VectorSearch(f => f.WithField(s => s.Vector), v => v.ByText("fruit"), minimumSimilarity: 0.75f)
                        .ToListAsync());

                Assert.Contains($"Embeddings Generation task with '{configuration.Identifier}' identifier is disabled, and cannot be used for querying", ex.Message);
            }
        }
    }

    private class TextualValueIndex : AbstractIndexCreationTask<Dto>
    {
        public TextualValueIndex()
        {
            Map = dtos => from dto in dtos
                select new { Vector = LoadVector("TextualValue", "localaitask") };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class Dto
    {
        public string TextualValue { get; set; }
        public object Vector { get; }
    }
}
