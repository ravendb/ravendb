using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23960(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DisabledTaskDoesntImpactCreationOfOtherTasks(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "Task1");
            var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            var op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.EmbeddingsGeneration);
            var taskInfo = (EmbeddingsGeneration)store.Maintenance.Send(op);
            
            store.Maintenance.Send(new ToggleOngoingTaskStateOperation(taskInfo.TaskId, OngoingTaskType.EmbeddingsGeneration, true));
                
            var (configuration2, _) = AddEmbeddingsGenerationTask(store, embeddingsGenerationTaskName: "Task2");
            (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration2);
            Assert.True(queriesWorkerRegistered);
            Assert.True(indexingWorkerRegistered);
            
            op = new GetOngoingTaskInfoOperation(configuration.Name, OngoingTaskType.EmbeddingsGeneration);
            taskInfo = (EmbeddingsGeneration)store.Maintenance.Send(op);

            Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);
            
            op = new GetOngoingTaskInfoOperation(configuration2.Name, OngoingTaskType.EmbeddingsGeneration);
            taskInfo = (EmbeddingsGeneration)store.Maintenance.Send(op);

            Assert.Equal(OngoingTaskState.Enabled, taskInfo.TaskState);
        }
    }
}
