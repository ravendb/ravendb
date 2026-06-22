using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Documents;
using Raven.Server.Documents.QueueSink;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Server;
using Xunit;

namespace SlowTests.Server.Documents.QueueSink
{
    [Trait("Category", "QueueSink")]
    public abstract class QueueSinkTestBase : RavenTestBase
    {
        protected QueueSinkTestBase(ITestOutputHelper output) : base(output)
        {
            QueueSuffix = Guid.NewGuid().ToString("N");
        }

        protected string QueueSuffix { get; }

        protected string UsersQueueName => $"users{QueueSuffix}";

        protected List<string> DefaultQueues => new() { UsersQueueName };

        protected AddQueueSinkOperationResult AddQueueSink<T>(DocumentStore src, QueueSinkConfiguration configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddQueueSinkOperation<T>(configuration));
            return addResult;
        }

        public static IEnumerable<QueueSinkErrorsDetails> GetAlerts(DocumentDatabase database, QueueSinkConfiguration config)
        {
            string tag = config.BrokerType switch
            {
                QueueBrokerType.Kafka => QueueSinkProcess.KafkaTag,
                QueueBrokerType.RabbitMq => QueueSinkProcess.RabbitMqTag,
                QueueBrokerType.AzureServiceBus => QueueSinkProcess.AzureServiceBusTag,
                _ => throw new NotSupportedException($"Unknown broker type: {config.BrokerType}")
            };

            foreach (var script in config.Scripts)
            {
                var processName = $"{config.Name}/{script.Name}";
                foreach (var error in database.NotificationCenter.QueueSinkNotifications.GetAlerts<QueueSinkErrorsDetails>(tag, processName))
                {
                    yield return error;
                }
            }
        }
        
        protected AsyncManualResetEvent WaitForQueueSinkBatch(DocumentStore store,
            Func<string, QueueSinkProcessStatistics, bool> predicate)
        {
            var database = AsyncHelpers.RunSync(() => GetDatabase(store.Database));

            var amre = new AsyncManualResetEvent();

            database.QueueSinkLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.ScriptName}", x.Statistics))
                    amre.Set();
            };

            return amre;
        }

        protected async Task AssertQueueSinkDoneAsync(AsyncManualResetEvent etlDone, TimeSpan timeout, string databaseName, QueueSinkConfiguration config)
        {
            if (await etlDone.WaitAsync(timeout) == false)
            {
                var database = await GetDatabase(databaseName);

                Assert.Fail(BuildSinkErrorMessage(database, config));
            }
        }

        public static string BuildSinkErrorMessage(DocumentDatabase database, QueueSinkConfiguration config)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"Queue Sink '{config.Name}' ({config.BrokerType}) did not complete on database '{database.Name}'.");
            sb.AppendLine($"Active processes: {database.QueueSinkLoader.Processes.Length}");
            foreach (var process in database.QueueSinkLoader.Processes)
            {
                var stats = process.Statistics;
                sb.AppendLine($"  - {process.Name}: ConsumeSuccesses={stats.ConsumeSuccesses}, ConsumeErrors={stats.ConsumeErrors}, WasLatestConsumeSuccessful={stats.WasLatestConsumeSuccessful}, LastConsumeErrorTime={stats.LastConsumeErrorTime}");
            }

            foreach (var alert in GetAlerts(database, config))
            {
                sb.AppendJoin(Environment.NewLine, alert.Errors.Select(x => $"{x.Date} : {x.Error}"));
            }

            return sb.ToString();
        }

        protected class User
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }

            public string FullName { get; set; }
        }
    }
}
