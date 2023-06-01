using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Documents.ETL.Providers.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Documents.QueueSink;
using Raven.Server.Documents.QueueSink.Test;
using Raven.Server.ServerWide.Context;
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

        [Fact]
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

        [Fact]
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
