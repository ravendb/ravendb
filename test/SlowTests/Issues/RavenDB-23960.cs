using Raven.Client.Documents.Operations.OngoingTasks;
using SlowTests.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23960(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DisabledTaskDoesntImpactCreationOfOtherTasks(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "Task1");
            
            var op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.EmbeddingsGeneration);
            var taskInfo = (EmbeddingsGeneration)store.Maintenance.Send(op);
            
            store.Maintenance.Send(new ToggleOngoingTaskStateOperation(taskInfo.TaskId, OngoingTaskType.EmbeddingsGeneration, true));
                
            var (configuration2, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "Task2");

            op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.EmbeddingsGeneration);
            taskInfo = (EmbeddingsGeneration)store.Maintenance.Send(op);

            Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);
            
            op = new GetOngoingTaskInfoOperation(configuration2.Name, OngoingTaskType.EmbeddingsGeneration);
            taskInfo = (EmbeddingsGeneration)store.Maintenance.Send(op);

            Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
        }
    }
}
