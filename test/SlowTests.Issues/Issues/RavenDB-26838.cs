using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.CdcSink;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Commands;
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
    public async Task FooterStatistics_CountsCdcSinkErrorsSeparately()
    {
        const string cdcSinkName = "CdcSink1";
        const string etlProcessName = "ETL1/Transformation1";

        using (var store = GetDocumentStore())
        {
            var database = await GetDatabase(store.Database);
            var now = DateTime.UtcNow;

            database.TaskErrorsStorage.StoreProcessError(TaskCategory.CdcSink, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = cdcSinkName,
                Step = TaskErrorStep.Extraction,
                Error = "consume error"
            });

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, cdcSinkName,
            [
                new TaskItemError { DocumentId = "orders/1", TaskName = cdcSinkName, CreatedAt = now, Step = TaskErrorStep.Load, Error = "e1" },
                new TaskItemError { DocumentId = "orders/2", TaskName = cdcSinkName, CreatedAt = now, Step = TaskErrorStep.Load, Error = "e2" }
            ]);

            // An ETL error in the same database must not be counted as a CDC Sink error.
            database.TaskErrorsStorage.StoreProcessError(TaskCategory.Etl, new TaskProcessError
            {
                CreatedAt = now,
                TaskName = etlProcessName,
                Step = TaskErrorStep.Transformation,
                Error = "etl error"
            });

            var stats = store.Maintenance.Send(new GetStudioFooterStatisticsOperation());

            Assert.NotNull(stats);
            Assert.Equal(3, stats.CountOfCdcSinkTasksErrors);
            Assert.Equal(1, stats.CountOfEtlTasksErrors);
            Assert.Equal(0, stats.CountOfAiTasksErrors);
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public async Task PerDocumentFailures_ArePersistedAsItemErrors()
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

                await process.SubmitBatchForTest(ops);
            }

            var itemErrors = database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName);
            Assert.Equal(100, itemErrors.Count);
            Assert.All(itemErrors, e => Assert.Equal((long)TaskErrorStep.Transformation, e.Step));
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void HealthStatus_BecomesFailedOnErrorsAndRecovers()
    {
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var stats = new CdcSinkProcessStatistics("CdcSink1", database.Configuration.CdcSink);

            Assert.Equal(EtlProcessHealthStatus.Healthy, stats.HealthStatus);

            // A fully-failed batch seeds the EWMA at ratio 1.0, so health drops to Failed.
            RunBatch(stats, errors: 100, successes: 0);
            Assert.Equal(EtlProcessHealthStatus.Failed, stats.HealthStatus);

            // Successful batches decay the ratio back below the thresholds, recovering to Healthy.
            for (int i = 0; i < 500 && stats.HealthStatus != EtlProcessHealthStatus.Healthy; i++)
                RunBatch(stats, errors: 0, successes: 100);

            Assert.Equal(EtlProcessHealthStatus.Healthy, stats.HealthStatus);
        }

        static void RunBatch(CdcSinkProcessStatistics stats, int errors, int successes)
        {
            stats.NewBatch();
            for (int i = 0; i < errors; i++)
                stats.RecordItemError(TaskErrorStep.Load, "error", "orders/" + i);
            if (successes > 0)
                stats.ConsumeSuccess(successes);
            stats.OnBatchCompletion();
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void HealthStatus_StaysFailedAfterPermanentFault()
    {
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var stats = new CdcSinkProcessStatistics("CdcSink1", database.Configuration.CdcSink);

            stats.SetHealthStatusToFailed();
            Assert.Equal(EtlProcessHealthStatus.Failed, stats.HealthStatus);

            // A clean batch must not clear a latched permanent-fault status.
            stats.NewBatch();
            stats.ConsumeSuccess(100);
            stats.OnBatchCompletion();

            Assert.Equal(EtlProcessHealthStatus.Failed, stats.HealthStatus);
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void HealthStatus_DegradesOnProcessFailures()
    {
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var stats = new CdcSinkProcessStatistics("CdcSink1", database.Configuration.CdcSink);

            Assert.Equal(EtlProcessHealthStatus.Healthy, stats.HealthStatus);

            // A process-level failure (e.g. the source is unreachable) records a consume error without
            // ever completing a batch. It must still degrade health - the first fully-failed sample
            // seeds the EWMA at ratio 1.0 - so a sink stuck reconnecting no longer reports Healthy.
            stats.RecordConsumeError();
            Assert.Equal(EtlProcessHealthStatus.Failed, stats.HealthStatus);
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public async Task HealthThresholds_MustHaveFailedGreaterThanImpaired()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new PutDatabaseSettingsOperation(store.Database, new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusFailedThreshold)] = "0.1",
                [RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusImpairedThreshold)] = "0.9"
            }));

            var ex = Assert.Throws<InvalidOperationException>(() =>
                Server.ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(store.Database));

            Assert.Contains(RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusFailedThreshold), ex.Message);
            Assert.Contains(RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusImpairedThreshold), ex.Message);
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void ErrorTables_AreDroppedWhenSinkDeleted()
    {
        const string taskName = "CdcSink1";

        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            var connectionString = new SqlConnectionString
            {
                Name = "cdc-cs",
                FactoryName = "Microsoft.Data.SqlClient",
                ConnectionString = "Server=localhost;Database=TestDb;User Id=sa;Password=pass;"
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(new CdcSinkConfiguration
            {
                Name = taskName,
                ConnectionStringName = connectionString.Name,
                Disabled = true,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping> { new CdcColumnMapping { Column = "order_id", Name = "OrderId" } },
                        PrimaryKeyColumns = new List<string> { "order_id" }
                    }
                }
            }));

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, taskName,
            [
                new TaskItemError { DocumentId = "orders/1", TaskName = taskName, CreatedAt = DateTime.UtcNow, Step = TaskErrorStep.Load, Error = "e1" }
            ]);
            Assert.NotEmpty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName));

            store.Maintenance.Send(new DeleteOngoingTaskOperation(addResult.TaskId, OngoingTaskType.CdcSink));

            // Deleting the sink removes its config from the record; the loader drops the dedicated error tables.
            Assert.Equal(0, WaitForValue(() => database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName).Count, 0, timeout: 15000, interval: 500));
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void ErrorTables_AreDroppedWhenSinkConfigurationChanges()
    {
        const string taskName = "CdcSink1";

        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            var connectionString = new SqlConnectionString
            {
                Name = "cdc-cs",
                FactoryName = "Microsoft.Data.SqlClient",
                ConnectionString = "Server=localhost;Database=TestDb;User Id=sa;Password=pass;"
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config = new CdcSinkConfiguration
            {
                Name = taskName,
                ConnectionStringName = connectionString.Name,
                Disabled = true,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping> { new CdcColumnMapping { Column = "order_id", Name = "OrderId" } },
                        PrimaryKeyColumns = new List<string> { "order_id" }
                    }
                }
            };

            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, taskName,
            [
                new TaskItemError { DocumentId = "orders/1", TaskName = taskName, CreatedAt = DateTime.UtcNow, Step = TaskErrorStep.Load, Error = "e1" }
            ]);
            Assert.NotEmpty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName));

            // Changing the configuration recreates the process on this node; the loader drops the stale error tables.
            config.TaskId = addResult.TaskId;
            config.Tables[0].SourceTableName = "updated_orders";
            store.Maintenance.Send(new UpdateCdcSinkOperation(addResult.TaskId, config));

            Assert.Equal(0, WaitForValue(() => database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName).Count, 0, timeout: 15000, interval: 500));
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void ErrorTables_ArePreservedWhenSinkOnlyDisabled()
    {
        const string taskName = "CdcSink1";

        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            var connectionString = new SqlConnectionString
            {
                Name = "cdc-cs",
                FactoryName = "Microsoft.Data.SqlClient",
                ConnectionString = "Server=localhost;Database=TestDb;User Id=sa;Password=pass;"
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config = new CdcSinkConfiguration
            {
                Name = taskName,
                ConnectionStringName = connectionString.Name,
                Disabled = false,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping> { new CdcColumnMapping { Column = "order_id", Name = "OrderId" } },
                        PrimaryKeyColumns = new List<string> { "order_id" }
                    }
                }
            };

            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            // Wait until the enabled sink is running on this node (the connection is bogus, so it just
            // retries - what matters is that the loader tracks its process).
            Assert.True(WaitForValue(() =>
            {
                var p = database.CdcSinkLoader.Processes.FirstOrDefault(x => string.Equals(x.Name, taskName, StringComparison.OrdinalIgnoreCase));
                return p != null && p.Configuration.Disabled == false;
            }, true, timeout: 15000, interval: 500));

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, taskName,
            [
                new TaskItemError { DocumentId = "orders/1", TaskName = taskName, CreatedAt = DateTime.UtcNow, Step = TaskErrorStep.Load, Error = "e1" }
            ]);
            Assert.NotEmpty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName));

            // Disabling a sink is not a data-affecting reconfigure: the loader must keep the error
            // history so an operator can inspect why the sink was failing before they disabled it.
            config.TaskId = addResult.TaskId;
            config.Disabled = true;
            store.Maintenance.Send(new UpdateCdcSinkOperation(addResult.TaskId, config));

            // Once the disabled replacement is in place the record change (and its cleanup) has run.
            Assert.True(WaitForValue(() =>
            {
                var p = database.CdcSinkLoader.Processes.FirstOrDefault(x => string.Equals(x.Name, taskName, StringComparison.OrdinalIgnoreCase));
                return p != null && p.Configuration.Disabled;
            }, true, timeout: 15000, interval: 500));

            Assert.NotEmpty(database.TaskErrorsStorage.ReadItemErrorsOfTask(TaskCategory.CdcSink, taskName));
        }
    }

    [RavenFact(RavenTestCategory.Monitoring | RavenTestCategory.Sinks)]
    public async Task CanGetCdcSinkErrorsSnmpMetrics_V2C()
    {
        var port = ReservePort().Port;
        var communityString = "public-test";
        var customSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)] = "true",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.SupportedVersions)] = "V2C",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Port)] = port.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Community)] = communityString
        };

        UseNewLocalServer(customSettings);

        using (var store = GetDocumentStore(new Options { CreateDatabase = true }))
        {
            const string cdcSinkName = "CdcSink1";

            var connectionString = new SqlConnectionString
            {
                Name = "cdc-cs",
                FactoryName = "Microsoft.Data.SqlClient",
                ConnectionString = "Server=localhost;Database=TestDb;User Id=sa;Password=pass;"
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            store.Maintenance.Send(new AddCdcSinkOperation(new CdcSinkConfiguration
            {
                Name = cdcSinkName,
                ConnectionStringName = connectionString.Name,
                Disabled = true,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping> { new CdcColumnMapping { Column = "order_id", Name = "OrderId" } },
                        PrimaryKeyColumns = new List<string> { "order_id" }
                    }
                }
            }));

            var database = GetDatabase(store.Database).GetAwaiter().GetResult();
            var now = DateTime.UtcNow;

            for (int i = 0; i < 2; i++)
            {
                database.TaskErrorsStorage.StoreProcessError(TaskCategory.CdcSink, new TaskProcessError
                {
                    CreatedAt = now,
                    TaskName = cdcSinkName,
                    Step = TaskErrorStep.Extraction,
                    Error = "process error"
                });
            }

            database.TaskErrorsStorage.StoreItemErrors(TaskCategory.CdcSink, cdcSinkName,
            [
                new TaskItemError { DocumentId = "orders/1", TaskName = cdcSinkName, CreatedAt = now, Step = TaskErrorStep.Load, Error = "e1" },
                new TaskItemError { DocumentId = "orders/2", TaskName = cdcSinkName, CreatedAt = now, Step = TaskErrorStep.Load, Error = "e2" },
                new TaskItemError { DocumentId = "orders/3", TaskName = cdcSinkName, CreatedAt = now, Step = TaskErrorStep.Transformation, Error = "e3" }
            ]);

            const int expectedErrors = 5;

            var ip = new Uri(Server.WebUrl).Host;
            var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);

            string serverErrorsOid = null, dbErrorsOid = null;
            string taskErrorsOid = null, taskHealthOid = null, taskResponsibleNodeOid = null, taskLastBatchOid = null;

            Assert.True(WaitForValue(() =>
            {
                using (var commands = store.Commands())
                {
                    var cmd = new GetSnmpOidsCommand();
                    commands.Execute(cmd);

                    if (cmd.Result is not BlittableJsonReaderObject res)
                        return false;

                    if (res.TryGet("Server", out BlittableJsonReaderArray serverEntries) == false ||
                        res.TryGet("Databases", out BlittableJsonReaderObject databases) == false ||
                        databases.TryGet(store.Database, out BlittableJsonReaderObject databaseOids) == false ||
                        databaseOids.TryGet("@General", out BlittableJsonReaderArray generalEntries) == false ||
                        databaseOids.TryGet("CdcSinks", out BlittableJsonReaderObject cdcSinks) == false ||
                        cdcSinks.TryGet(cdcSinkName, out BlittableJsonReaderArray cdcSinkEntries) == false ||
                        cdcSinkEntries == null)
                        return false;

                    string Find(BlittableJsonReaderArray entries, string description) =>
                        JsonConvert.DeserializeObject<List<SnmpEntry>>(entries.ToString()).SingleOrDefault(x => x.Description == description)?.OID;

                    serverErrorsOid = Find(serverEntries, "Number of CDC Sink errors");
                    dbErrorsOid = Find(generalEntries, "Number of CDC Sink errors");
                    taskErrorsOid = Find(cdcSinkEntries, "Number of CDC Sink task errors");
                    taskHealthOid = Find(cdcSinkEntries, "Health status of particular CDC Sink task");
                    taskResponsibleNodeOid = Find(cdcSinkEntries, "Responsible node tag of particular CDC Sink task");
                    taskLastBatchOid = Find(cdcSinkEntries, "Last successful batch time");

                    return serverErrorsOid != null && dbErrorsOid != null && taskErrorsOid != null &&
                           taskHealthOid != null && taskResponsibleNodeOid != null && taskLastBatchOid != null;
                }
            }, true, timeout: 20000, interval: 500));

            ISnmpData SnmpGet(string oid)
            {
                var result = Messenger.Get(VersionCode.V2, endpoint, new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(oid))], 10000);
                return result.Single().Data;
            }

            Assert.Equal(expectedErrors, ((Integer32)SnmpGet(serverErrorsOid)).ToInt32());
            Assert.Equal(expectedErrors, ((Integer32)SnmpGet(dbErrorsOid)).ToInt32());
            Assert.Equal(expectedErrors, ((Integer32)SnmpGet(taskErrorsOid)).ToInt32());

            Assert.Equal(nameof(EtlProcessHealthStatus.Healthy), SnmpGet(taskHealthOid).ToString());

            Assert.Equal(Server.ServerStore.NodeTag, SnmpGet(taskResponsibleNodeOid).ToString());

            Assert.Equal(SnmpType.TimeTicks, SnmpGet(taskLastBatchOid).TypeCode);
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
