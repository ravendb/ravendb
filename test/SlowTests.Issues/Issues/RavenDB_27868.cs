using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.ETL.Providers.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.Kafka;
using Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27868 : RavenTestBase
    {
        public RavenDB_27868(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task SqlEtl_restarts_when_only_target_specific_fields_change()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewSqlConfiguration();
                var taskId = Etl.AddEtl(store, config, NewSqlConnectionString()).TaskId;

                config.SqlTables.Add(new SqlEtlTable { TableName = "Orders", DocumentIdColumn = "Id" });
                config.CommandTimeout = 77;
                var process = await UpdateAndWaitForNewProcess<SqlEtl, SqlConnectionString>(store, taskId, config);

                Assert.Equal(2, process.Configuration.SqlTables.Count);
                Assert.Equal(77, process.Configuration.CommandTimeout);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task SqlEtl_keeps_running_process_when_tables_are_only_reordered()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewSqlConfiguration();
                config.SqlTables.Add(new SqlEtlTable { TableName = "Orders", DocumentIdColumn = "Id" });
                var taskId = Etl.AddEtl(store, config, NewSqlConnectionString()).TaskId;

                config.SqlTables.Reverse();
                await UpdateAndAssertSameProcess(store, taskId, config);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task ElasticSearchEtl_restarts_when_only_indexes_change()
        {
            using (var store = GetDocumentStore())
            {
                var config = new ElasticSearchEtlConfiguration
                {
                    Name = "es",
                    ConnectionStringName = "es-cs",
                    ElasticIndexes = { new ElasticSearchIndex { IndexName = "users", DocumentIdProperty = "Id" } },
                    Transforms = { NewTransformation() }
                };
                var taskId = Etl.AddEtl(store, config, new ElasticSearchConnectionString { Name = "es-cs", Nodes = new[] { "http://localhost:1234" } }).TaskId;

                config.ElasticIndexes[0].InsertOnlyMode = true;
                var process = await UpdateAndWaitForNewProcess<ElasticSearchEtl, ElasticSearchConnectionString>(store, taskId, config);

                Assert.True(process.Configuration.ElasticIndexes[0].InsertOnlyMode);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task KafkaEtl_restarts_when_only_queues_change()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewQueueConfiguration("kafka", QueueBrokerType.Kafka);
                var taskId = Etl.AddEtl(store, config, NewKafkaConnectionString()).TaskId;

                config.Queues[0].DeleteProcessedDocuments = true;
                config.SkipAutomaticQueueDeclaration = true;
                var process = await UpdateAndWaitForNewProcess<QueueEtl<KafkaItem>, QueueConnectionString>(store, taskId, config);

                Assert.True(process.Configuration.Queues[0].DeleteProcessedDocuments);
                Assert.True(process.Configuration.SkipAutomaticQueueDeclaration);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task KafkaEtl_keeps_running_process_when_nothing_changes()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewQueueConfiguration("kafka", QueueBrokerType.Kafka);
                var connectionString = NewKafkaConnectionString();
                connectionString.KafkaConnectionSettings.ConnectionOptions = new Dictionary<string, string> { ["client.id"] = "raven" };
                var taskId = Etl.AddEtl(store, config, connectionString).TaskId;

                await UpdateAndAssertSameProcess(store, taskId, config);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task RabbitMqEtl_restarts_when_only_queues_change()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewQueueConfiguration("rabbit", QueueBrokerType.RabbitMq);
                var taskId = Etl.AddEtl(store, config, new QueueConnectionString
                {
                    Name = "rabbit-cs",
                    BrokerType = QueueBrokerType.RabbitMq,
                    RabbitMqConnectionSettings = new RabbitMqConnectionSettings { ConnectionString = "amqp://guest:guest@localhost:1234/" }
                }).TaskId;

                config.Queues.Add(new EtlQueue { Name = "Orders" });
                var process = await UpdateAndWaitForNewProcess<QueueEtl<RabbitMqItem>, QueueConnectionString>(store, taskId, config);

                Assert.Equal(2, process.Configuration.Queues.Count);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task SnowflakeEtl_restarts_when_only_target_specific_fields_change()
        {
            using (var store = GetDocumentStore())
            {
                var config = new SnowflakeEtlConfiguration
                {
                    Name = "snow",
                    ConnectionStringName = "snow-cs",
                    SnowflakeTables = { new SnowflakeEtlTable { TableName = "Users", DocumentIdColumn = "Id" } },
                    Transforms = { NewTransformation() }
                };
                var taskId = Etl.AddEtl(store, config, new SnowflakeConnectionString { Name = "snow-cs", ConnectionString = "account=x;user=y;password=z;" }).TaskId;

                config.SnowflakeTables.Add(new SnowflakeEtlTable { TableName = "Orders", DocumentIdColumn = "Id" });
                config.CommandTimeout = 77;
                var process = await UpdateAndWaitForNewProcess<SnowflakeEtl, SnowflakeConnectionString>(store, taskId, config);

                Assert.Equal(2, process.Configuration.SnowflakeTables.Count);
                Assert.Equal(77, process.Configuration.CommandTimeout);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task OlapEtl_restarts_when_only_target_specific_fields_change()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewOlapConfiguration();
                var taskId = Etl.AddEtl(store, config, NewOlapConnectionString()).TaskId;

                config.OlapTables.Add(new OlapEtlTable { TableName = "Orders", DocumentIdColumn = "Id" });
                config.CustomPartitionValue = "partition";
                var process = await UpdateAndWaitForNewProcess<OlapEtl, OlapConnectionString>(store, taskId, config);

                Assert.Equal(2, process.Configuration.OlapTables.Count);
                Assert.Equal("partition", process.Configuration.CustomPartitionValue);
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task OlapEtl_keeps_running_process_when_tables_are_only_reordered()
        {
            using (var store = GetDocumentStore())
            {
                var config = NewOlapConfiguration();
                config.OlapTables.Add(new OlapEtlTable { TableName = "Orders", DocumentIdColumn = "Id" });
                var taskId = Etl.AddEtl(store, config, NewOlapConnectionString()).TaskId;

                config.OlapTables.Reverse();
                await UpdateAndAssertSameProcess(store, taskId, config);
            }
        }

        private static Transformation NewTransformation() => new Transformation
        {
            Name = "Users",
            Collections = new List<string> { "Users" },
            Script = "loadToUsers({ Name: this.Name });"
        };

        private static SqlEtlConfiguration NewSqlConfiguration() => new SqlEtlConfiguration
        {
            Name = "sql",
            ConnectionStringName = "sql-cs",
            SqlTables = { new SqlEtlTable { TableName = "Users", DocumentIdColumn = "Id" } },
            Transforms = { NewTransformation() }
        };

        private OlapEtlConfiguration NewOlapConfiguration() => new OlapEtlConfiguration
        {
            Name = "olap",
            ConnectionStringName = "olap-cs",
            RunFrequency = "0 0 * * *",
            OlapTables = new List<OlapEtlTable> { new OlapEtlTable { TableName = "Users", DocumentIdColumn = "Id" } },
            Transforms = { NewTransformation() }
        };

        private OlapConnectionString NewOlapConnectionString() => new OlapConnectionString
        {
            Name = "olap-cs",
            LocalSettings = new LocalSettings { FolderPath = NewDataPath() }
        };

        private static SqlConnectionString NewSqlConnectionString() => new SqlConnectionString
        {
            Name = "sql-cs",
            ConnectionString = "Server=localhost;Database=x;",
            FactoryName = "Microsoft.Data.SqlClient"
        };

        private static QueueEtlConfiguration NewQueueConfiguration(string name, QueueBrokerType brokerType) => new QueueEtlConfiguration
        {
            Name = name,
            ConnectionStringName = $"{name}-cs",
            BrokerType = brokerType,
            Queues = { new EtlQueue { Name = "Users" } },
            Transforms = { NewTransformation() }
        };

        private static QueueConnectionString NewKafkaConnectionString() => new QueueConnectionString
        {
            Name = "kafka-cs",
            BrokerType = QueueBrokerType.Kafka,
            KafkaConnectionSettings = new KafkaConnectionSettings { BootstrapServers = "localhost:1234" }
        };

        private async Task<TProcess> UpdateAndWaitForNewProcess<TProcess, TConnectionString>(DocumentStore store, long taskId, EtlConfiguration<TConnectionString> configuration)
            where TProcess : EtlProcess
            where TConnectionString : ConnectionString
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var before = Assert.Single(database.EtlLoader.Processes, x => x.ConfigurationName == configuration.Name);

            var result = store.Maintenance.Send(new UpdateEtlOperation<TConnectionString>(taskId, configuration));
            await Databases.WaitForRaftIndex(store.Database, result.RaftCommandIndex);

            var after = await WaitForPredicateAsync(process => ReferenceEquals(before, process) == false,
                () => Task.FromResult(Assert.Single(database.EtlLoader.Processes, x => x.ConfigurationName == configuration.Name)), interval: 333);

            Assert.NotSame(before, after);
            return Assert.IsAssignableFrom<TProcess>(after);
        }

        private async Task UpdateAndAssertSameProcess<TConnectionString>(DocumentStore store, long taskId, EtlConfiguration<TConnectionString> configuration)
            where TConnectionString : ConnectionString
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var before = Assert.Single(database.EtlLoader.Processes, x => x.ConfigurationName == configuration.Name);

            var result = store.Maintenance.Send(new UpdateEtlOperation<TConnectionString>(taskId, configuration));
            await Databases.WaitForRaftIndex(store.Database, result.RaftCommandIndex);
            await Task.Delay(TimeSpan.FromSeconds(1));

            var after = Assert.Single(database.EtlLoader.Processes, x => x.ConfigurationName == configuration.Name);
            Assert.Same(before, after);
        }
    }
}
