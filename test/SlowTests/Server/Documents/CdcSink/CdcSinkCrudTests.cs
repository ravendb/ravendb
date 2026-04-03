using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkCrudTests : RavenTestBase
    {
        public CdcSinkCrudTests(ITestOutputHelper output) : base(output)
        {
        }

        private static SqlConnectionString CreateSqlConnectionString(string name = "test-sql")
        {
            return new SqlConnectionString
            {
                Name = name,
                FactoryName = "System.Data.SqlClient",
                ConnectionString = "Server=localhost;Database=TestDb;User Id=sa;Password=pass;"
            };
        }

        private static CdcSinkConfiguration CreateCdcSinkConfiguration(string name, string connectionStringName)
        {
            return new CdcSinkConfiguration
            {
                Name = name,
                ConnectionStringName = connectionStringName,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
                            new CdcColumnMapping { Column = "customer_id", Name = "CustomerId" }
                        },
                        PrimaryKeyColumns = new List<string> { "order_id" },
                    }
                }
            };
        }

        [Fact]
        public async Task CanAddCdcSink()
        {
            using var store = GetDocumentStore();

            var connectionString = CreateSqlConnectionString();
            var putResult = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var config = CreateCdcSinkConfiguration("test-cdc", connectionString.Name);
            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            Assert.NotNull(addResult);
            Assert.True(addResult.TaskId > 0);

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(1, record.CdcSinks.Count);
            Assert.Equal("test-cdc", record.CdcSinks[0].Name);
            Assert.Equal(connectionString.Name, record.CdcSinks[0].ConnectionStringName);
        }

        [Fact]
        public async Task CanUpdateCdcSink()
        {
            using var store = GetDocumentStore();

            var connectionString = CreateSqlConnectionString();
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config = CreateCdcSinkConfiguration("test-cdc", connectionString.Name);
            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            // Update the configuration
            config.TaskId = addResult.TaskId;
            config.Tables[0].SourceTableName = "updated_orders";

            var updateResult = store.Maintenance.Send(new UpdateCdcSinkOperation(addResult.TaskId, config));
            Assert.NotNull(updateResult);

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(1, record.CdcSinks.Count);
            Assert.Equal("updated_orders", record.CdcSinks[0].Tables[0].SourceTableName);
        }

        [Fact]
        public async Task CanDeleteCdcSink()
        {
            using var store = GetDocumentStore();

            var connectionString = CreateSqlConnectionString();
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config = CreateCdcSinkConfiguration("test-cdc", connectionString.Name);
            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            store.Maintenance.Send(new DeleteOngoingTaskOperation(addResult.TaskId, OngoingTaskType.CdcSink));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(0, record.CdcSinks.Count);
        }

        [Fact]
        public void CanGetCdcSinkTaskInfo()
        {
            using var store = GetDocumentStore();

            var connectionString = CreateSqlConnectionString();
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config = CreateCdcSinkConfiguration("test-cdc", connectionString.Name);
            config.Disabled = true;
            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            var op = new GetOngoingTaskInfoOperation(config.Name, OngoingTaskType.CdcSink);
            var taskInfo = (OngoingTaskCdcSink)store.Maintenance.Send(op);

            Assert.NotNull(taskInfo);
            Assert.Null(taskInfo.Error);
            Assert.Equal(OngoingTaskState.Disabled, taskInfo.TaskState);
            Assert.True(taskInfo.Configuration.Disabled);
            Assert.Equal(connectionString.Name, taskInfo.ConnectionStringName);

            var nonExisting = new GetOngoingTaskInfoOperation("non-existing", OngoingTaskType.CdcSink);
            var nullTaskInfo = (OngoingTaskCdcSink)store.Maintenance.Send(nonExisting);
            Assert.Null(nullTaskInfo);
        }

        [Fact]
        public async Task CanToggleCdcSinkState()
        {
            using var store = GetDocumentStore();

            var connectionString = CreateSqlConnectionString();
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config = CreateCdcSinkConfiguration("test-cdc", connectionString.Name);
            var addResult = store.Maintenance.Send(new AddCdcSinkOperation(config));

            // Disable
            store.Maintenance.Send(new ToggleOngoingTaskStateOperation(addResult.TaskId, OngoingTaskType.CdcSink, disable: true));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.True(record.CdcSinks[0].Disabled);

            // Re-enable
            store.Maintenance.Send(new ToggleOngoingTaskStateOperation(addResult.TaskId, OngoingTaskType.CdcSink, disable: false));

            record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.False(record.CdcSinks[0].Disabled);
        }

        [Fact]
        public async Task CanAddMultipleCdcSinks()
        {
            using var store = GetDocumentStore();

            var connectionString = CreateSqlConnectionString();
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

            var config1 = CreateCdcSinkConfiguration("cdc-sink-1", connectionString.Name);
            var config2 = CreateCdcSinkConfiguration("cdc-sink-2", connectionString.Name);

            store.Maintenance.Send(new AddCdcSinkOperation(config1));
            store.Maintenance.Send(new AddCdcSinkOperation(config2));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(2, record.CdcSinks.Count);

            var names = record.CdcSinks.Select(x => x.Name).ToList();
            Assert.Contains("cdc-sink-1", names);
            Assert.Contains("cdc-sink-2", names);
        }
    }
}
