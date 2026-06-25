using System;
using System.Linq;
using FastTests;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26838 : RavenTestBase
{
    public RavenDB_26838(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void CdcSinkErrors_RoundTripThroughDedicatedStorage()
    {
        const string taskName = "CdcSink1";

        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var now = DateTime.UtcNow;

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.CdcSink, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = taskName,
                AffectedDocumentsCount = 0,
                Step = TaskErrorStep.Configuration,
                Error = "configuration error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, taskName,
            [
                new TaskItemError
                {
                    DocumentId = "orders/1",
                    TaskName = taskName,
                    CreatedAt = now,
                    Step = TaskErrorStep.Transformation,
                    Error = "script error"
                }
            ]);

            var processErrors = database.TaskErrorsStorage.ReadProcessErrorsOfTask(TaskCategory.CdcSink, taskName);
            var itemErrors = database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName);

            Assert.Single(processErrors);
            Assert.Equal(taskName, processErrors[0].TaskName);
            Assert.Equal((long)TaskErrorStep.Configuration, processErrors[0].Step);
            Assert.Equal("configuration error", processErrors[0].Error);

            Assert.Single(itemErrors);
            Assert.Equal("orders/1", itemErrors[0].DocumentId);
            Assert.Equal((long)TaskErrorStep.Transformation, itemErrors[0].Step);
            Assert.Equal("script error", itemErrors[0].Error);

            // Errors stored under CdcSink must not leak into the Etl / Ai categories (separate Voron tables).
            Assert.Empty(database.TaskErrorsStorage.ReadProcessErrorsOfTask(TaskCategory.Etl, taskName));
            Assert.Empty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.Etl, taskName));

            // The grouped read used by the errors endpoints surfaces the CDC task.
            var grouped = database.TaskErrorsStorage.ReadAllErrorsGroupedByTask(TaskCategory.CdcSink);
            Assert.Contains(grouped, x => x.TaskName == taskName && x.ProcessErrors.Count == 1 && x.ItemErrors.Count == 1);

            database.TaskErrorsStorage.DeleteErrorsOfTask(taskName, TaskCategory.CdcSink);

            Assert.Empty(database.TaskErrorsStorage.ReadProcessErrorsOfTask(TaskCategory.CdcSink, taskName));
            Assert.Empty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName));
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void GetCdcSinkErrors_EndpointReturnsStoredErrors()
    {
        const string taskName = "CdcSink1";

        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var now = DateTime.UtcNow;

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.CdcSink, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = taskName,
                AffectedDocumentsCount = 3,
                Step = TaskErrorStep.Extraction,
                Error = "consume error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, taskName,
            [
                new TaskItemError
                {
                    DocumentId = "orders/2",
                    TaskName = taskName,
                    CreatedAt = now,
                    Step = TaskErrorStep.Load,
                    Error = "apply error"
                }
            ]);

            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new GetTaskErrorsCommand(names: null, TaskCategory.CdcSink, database.ServerStore.NodeTag);
                requestExecutor.Execute(command, context);

                var task = command.Result.Single(x => x.TaskName == taskName);

                // CDC sinks are not ETL processes, so the shared DTO carries no EtlType, only the category.
                Assert.Equal(TaskCategory.CdcSink, task.Category);
                Assert.Null(task.EtlType);
                Assert.Single(task.ProcessErrors);
                Assert.Equal(TaskErrorStep.Extraction, task.ProcessErrors[0].Step);
                Assert.Single(task.ItemErrors);
                Assert.Equal("orders/2", task.ItemErrors[0].DocumentId);
                Assert.Equal(TaskErrorStep.Load, task.ItemErrors[0].Step);
            }
        }
    }
}
