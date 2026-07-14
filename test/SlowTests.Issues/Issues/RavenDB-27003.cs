using System;
using FastTests;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.Documents.TasksErrors;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27003 : RavenTestBase
{
    public RavenDB_27003(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void DatabaseStats_IncludeTaskErrorCountsSoTheFooterBadgeUpdatesLive()
    {
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var now = DateTime.UtcNow;

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.CdcSink, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = "CdcSink1",
                Step = TaskErrorStep.Extraction,
                Error = "consume error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, "CdcSink1",
            [
                new TaskItemError { DocumentId = "orders/1", TaskName = "CdcSink1", CreatedAt = now, Step = TaskErrorStep.Load, Error = "e1" }
            ]);

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = "Etl1/Transformation1",
                Step = TaskErrorStep.Transformation,
                Error = "etl error"
            });
            
            var stats = DatabaseStatsSender.GetStats(database);

            Assert.Equal(2, stats.CountOfCdcSinkTasksErrors);
            Assert.Equal(1, stats.CountOfEtlTasksErrors);
            Assert.Equal(0, stats.CountOfAiTasksErrors);
        }
    }
}
