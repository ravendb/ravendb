using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents;
using Raven.Server.Documents.CdcSink;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Raven.Server.Documents.TasksErrors;

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

    [RavenFact(RavenTestCategory.Sinks)]
    public async Task FailedBatch_StillPersistsBufferedItemErrors()
    {
        const string taskName = "CdcSink-failing";

        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            var config = new CdcSinkConfiguration
            {
                Name = taskName,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        PrimaryKeyColumns = new List<string> { "order_id" },
                        Patch = "throw new Error('intentional failure');"
                    }
                }
            };

            using var process = new TestCdcSinkProcess(config, database);
            var docProcessor = process.TestDocumentProcessor;
            docProcessor.SetSourceColumnNames("public", "orders", new[] { "order_id", "customer_name" });
            var tableProcessor = docProcessor.GetProcessor("public", "orders");

            var ops = new List<CdcSinkDocumentOp>();
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                for (int i = 1; i <= 100; i++)
                {
                    var data = new object[] { i, "name" + i };
                    ops.Add(docProcessor.ProcessRow(tableProcessor, CdcSinkOperation.Upsert, data, context));
                }

                await Assert.ThrowsAnyAsync<Exception>(() => process.SubmitBatchForTest(ops));
            }

            var itemErrors = database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName);
            Assert.NotEmpty(itemErrors);
            Assert.All(itemErrors, e => Assert.Equal((long)TaskErrorStep.Transformation, e.Step));
        }
    }

    private sealed class TestCdcSinkProcess : CdcSinkProcess
    {
        public TestCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
            : base(configuration, database, defaultSchema: "public")
        {
        }

        public CdcSinkDocumentProcessor TestDocumentProcessor => DocumentProcessor;

        public Task<(string Checkpoint, int Rows)> SubmitBatchForTest(List<CdcSinkDocumentOp> ops) => SubmitBatch(ops);

        public override bool IsHealthy(out string issue)
        {
            issue = null;
            return true;
        }

        protected override Task RunInternalAsync(CancellationToken ct) => throw new NotSupportedException();

        protected override IAsyncEnumerable<CdcEvent> GetCdcEvents(CancellationToken ct) => throw new NotSupportedException();

        protected override string GetDefaultSchema() => "public";

        protected override Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct) => throw new NotSupportedException();

        protected override Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct) => throw new NotSupportedException();

        protected override object ConvertInitialLoadValue(DbDataReader reader, int ordinal, CdcSinkConfiguration.TableInfo tableInfo) => throw new NotSupportedException();

        protected override DbCommandBuilder CommandBuilder => null;

        public override void Dispose()
        {
        }
    }
}
