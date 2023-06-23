using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.QueueSink;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.QueueSink.Kafka
{
    public class KafkaQueueSinkTests : QueueSinkTestBase
    {
        public KafkaQueueSinkTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string DefaultScript = "put(this.Id, this)";
        private readonly List<string> _defaultQueue = new() { "users" };

        [RequiresKafkaRetryFact]
        public void SimpleScript()
        {
            var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
            var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

            byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
            byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

            var kafkaMessage1 = new Message<string, byte[]> { Value = userBytes1 };
            var kafkaMessage2 = new Message<string, byte[]> { Value = userBytes2 };

            using IProducer<string, byte[]> producer = CreateKafkaProducer();

            producer.Produce("users", kafkaMessage1);
            producer.Produce("users", kafkaMessage2);

            using var store = GetDocumentStore();
            SetupKafkaQueueSink(store, "put(this.Id, this)", new List<string>() { "users" });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
            AssertQueueSinkDone(etlDone, TimeSpan.FromMinutes(1));

            using var session = store.OpenSession();

            var fetchedUser1 = session.Load<User>("users/1");
            Assert.NotNull(fetchedUser1);
            Assert.Equal("users/1", fetchedUser1.Id);
            Assert.Equal("John", fetchedUser1.FirstName);
            Assert.Equal("Doe", fetchedUser1.LastName);

            var fetchedUser2 = session.Load<User>("users/2");
            Assert.NotNull(fetchedUser2);
            Assert.Equal("users/2", fetchedUser2.Id);
            Assert.Equal("Jane", fetchedUser2.FirstName);
            Assert.Equal("Smith", fetchedUser2.LastName);
        }

        [RequiresKafkaRetryFact]
        public void ComplexScript()
        {
            var script =
                @"var item = { Id : this.Id, FirstName : this.FirstName, LastName : this.LastName, FullName : this.FirstName + ' ' + this.LastName }
                 put(this.Id, item)";

            var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
            var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

            byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
            byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

            var kafkaMessage1 = new Message<string, byte[]> { Value = userBytes1 };
            var kafkaMessage2 = new Message<string, byte[]> { Value = userBytes2 };

            using IProducer<string, byte[]> producer = CreateKafkaProducer();

            producer.Produce("users", kafkaMessage1);
            producer.Produce("users", kafkaMessage2);

            using var store = GetDocumentStore();
            SetupKafkaQueueSink(store, script, new List<string>() { "users" });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
            AssertQueueSinkDone(etlDone, TimeSpan.FromSeconds(20));

            using var session = store.OpenSession();

            var fetchedUser1 = session.Load<User>("users/1");
            Assert.NotNull(fetchedUser1);
            Assert.Equal("users/1", fetchedUser1.Id);
            Assert.Equal("John", fetchedUser1.FirstName);
            Assert.Equal("Doe", fetchedUser1.LastName);
            Assert.Equal("John Doe", fetchedUser1.FullName);

            var fetchedUser2 = session.Load<User>("users/2");
            Assert.NotNull(fetchedUser2);
            Assert.Equal("users/2", fetchedUser2.Id);
            Assert.Equal("Jane", fetchedUser2.FirstName);
            Assert.Equal("Smith", fetchedUser2.LastName);
            Assert.Equal("Jane Smith", fetchedUser2.FullName);
        }
        
        [RequiresKafkaRetryFact]
        public void SimpleScriptMultipleInserts()
        {
            var numberOfUsers = 10;

            using IProducer<string, byte[]> producer = CreateKafkaProducer();
            
            for (int i = 0; i < numberOfUsers; i++)
            {
                var user = new User { Id = $"users/{i}", FirstName = $"firstname{i}", LastName = $"lastname{i}" };
                byte[] userBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user));
                var kafkaMessage = new Message<string, byte[]> { Value = userBytes };
                producer.Produce("users", kafkaMessage);
            }
            
            using var store = GetDocumentStore();
            SetupKafkaQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)", new List<string>() { "users" });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
            AssertQueueSinkDone(etlDone, TimeSpan.FromMinutes(1));

            using var session = store.OpenSession();

            var users = session.Query<User>().ToList();
            Assert.Equal(numberOfUsers, users.Count);

            for (int i = 0; i < numberOfUsers; i++)
            {
                var fetchedUser = session.Load<User>($"users/{i}");
                Assert.NotNull(fetchedUser);
                Assert.Equal($"users/{i}", fetchedUser.Id);
                Assert.Equal($"firstname{i}", fetchedUser.FirstName);
                Assert.Equal($"lastname{i}", fetchedUser.LastName);    
            }
        }

        [Fact]
        public void Error_if_script_is_empty()
        {
            var config = new QueueSinkConfiguration
            {
                Name = "test",
                ConnectionStringName = "test",
                BrokerType = QueueBrokerType.Kafka,
                Scripts = { new QueueSinkScript { Name = "test", Script = @"" } }
            };

            config.Initialize(new QueueConnectionString
            {
                Name = "Foo",
                BrokerType = QueueBrokerType.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings()
                {
                    ConnectionOptions = new Dictionary<string, string> { }, BootstrapServers = "localhost:29092"
                }
            });

            List<string> errors;
            config.Validate(out errors);

            Assert.Equal(1, errors.Count);

            Assert.Equal("Script 'test' must not be empty", errors[0]);
        }

        [Fact]
        public async Task ShouldImportTask()
        {
            using (var srcStore = GetDocumentStore())
            using (var dstStore = GetDocumentStore())
            {
                SetupKafkaQueueSink(dstStore, DefaultScript, _defaultQueue, bootstrapServers: "http://localhost:1234");

                var exportFile = GetTempFileName();

                var exportOperation =
                    await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var destinationRecord =
                    await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
                Assert.Equal(1, destinationRecord.QueueConnectionStrings.Count);
                Assert.Equal(1, destinationRecord.QueueSinks.Count);
                Assert.Equal(1, destinationRecord.QueueSinks[0].Scripts.Count);

                Assert.Equal(QueueBrokerType.Kafka, destinationRecord.QueueSinks[0].BrokerType);
                Assert.Equal(DefaultScript, destinationRecord.QueueSinks[0].Scripts[0].Script);
                Assert.Equal(_defaultQueue, destinationRecord.QueueSinks[0].Scripts[0].Queues);
                
            }
        }
        
        [Fact]
        public async Task Simple_script_error_expected()
        {
            using (var store = GetDocumentStore())
            {
                var config = new QueueSinkConfiguration
                {
                    Name = "test",
                    ConnectionStringName = "test",
                    BrokerType = QueueBrokerType.Kafka,
                    Scripts = { new QueueSinkScript { Name = "test", Script = DefaultScript } }
                };

                AddQueueSink(store, config,
                    new QueueConnectionString
                    {
                        Name = "test",
                        BrokerType = QueueBrokerType.Kafka,
                        KafkaConnectionSettings =
                            new KafkaConnectionSettings() { BootstrapServers = "http://localhost:1234" }
                    }); //wrong bootstrap servers

                var alert = await AssertWaitForNotNullAsync(() =>
                {
                    TryGetLoadError(store.Database, config, out var error);

                    return Task.FromResult(error);
                }, timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);
                
                Assert.StartsWith("Raven.Server.Exceptions.ETL.ElasticSearch.ElasticSearchLoadException", alert.Error);
            }
        }

        /*[Fact]
        public async Task CanTestScript()
        {
            using (var store = GetDocumentStore())
            {
                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(
                    new QueueConnectionString
                    {
                        Name = "simulate",
                        BrokerType = QueueBrokerType.Kafka,
                        KafkaConnectionSettings =
                            new KafkaConnectionSettings() { BootstrapServers = "localhost:29092" }
                    }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(
                           out DocumentsOperationContext context))
                {
                    using (QueueSinkProcess.TestScript(
                               new TestQueueSinkScript()
                               {
                                   Configuration = new QueueSinkConfiguration
                                   {
                                       Name = "simulate",
                                       ConnectionStringName = "simulate",
                                       BrokerType = QueueBrokerType.Kafka,
                                       Scripts =
                                       {
                                           new QueueSinkScript
                                           {
                                               Queues = new List<string>() { "users" },
                                               Name = "users",
                                               Script = @"put(this.Id, this)"
                                           }
                                       }
                                   }
                               }, context, out var testResult))
                    {

                        Assert.Equal(0, testResult.ScriptErrors.Count);

                        //Assert.Equal(1, result.Summary.Count);

                        Assert.Equal("test output", testResult.DebugOutput[0]);
                    }
                }
            }
        }*/

        protected void SetupKafkaQueueSink(DocumentStore store, string script, List<string> queues,
            string configurationName = null,
            string transformationName = null, Dictionary<string, string> configuration = null,
            string bootstrapServers = null)
        {
            var connectionStringName = $"{store.Database} to Kafka";

            QueueSinkScript queueSinkScript = new QueueSinkScript
            {
                Name = transformationName ?? $"Queue Sink : {connectionStringName}",
                Queues = new List<string>(queues),
                Script = script,
            };
            var config = new QueueSinkConfiguration
            {
                Name = configurationName ?? connectionStringName,
                ConnectionStringName = connectionStringName,
                Scripts = { queueSinkScript },
                BrokerType = QueueBrokerType.Kafka
            };

            AddQueueSink(store, config,
                new QueueConnectionString
                {
                    Name = connectionStringName,
                    BrokerType = QueueBrokerType.Kafka,
                    KafkaConnectionSettings = new KafkaConnectionSettings
                    {
                        ConnectionOptions = configuration,
                        BootstrapServers = bootstrapServers ?? KafkaConnectionString.Instance.VerifiedUrl.Value,
                    }
                });
        }

        public static IProducer<string, byte[]> CreateKafkaProducer(string bootstrapServers = null)
        {
            ProducerConfig config = new()
            {
                BootstrapServers = bootstrapServers ?? KafkaConnectionString.Instance.VerifiedUrl.Value,
                EnableIdempotence = true
            };

            IProducer<string, byte[]> producer = new ProducerBuilder<string, byte[]>(config).Build();
            return producer;
        }

        private ManualResetEventSlim WaitForEtl(DocumentStore store,
            Func<string, QueueSinkProcessStatistics, bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.QueueSinkLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };

            return mre;
        }

        private void AssertQueueSinkDone(ManualResetEventSlim etlDone, TimeSpan timeout)
        {
            if (etlDone.Wait(timeout) == false)
            {
                //TryGetLoadError(databaseName, config, out var loadError);
                //TryGetTransformationError(databaseName, config, out var transformationError);

                //Assert.True(false, $"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
            }
        }
    }

    public class User
    {
        public string Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }

        public string FullName { get; set; }
    }
}
