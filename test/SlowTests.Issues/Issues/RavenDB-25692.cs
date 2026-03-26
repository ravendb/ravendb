using System.Threading.Tasks;
using Raven.Client.Documents;
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
    public async Task QueryingDisabledEmbeddings(Options options)
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
        }
    }
    
    private class Dto
    {
        public string TextualValue { get; set; }
    }
}
