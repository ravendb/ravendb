using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using QueueSinkConfiguration = Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration;

namespace SlowTests.Server.Documents.QueueSink
{
    [Trait("Category", "QueueSink")]
    public abstract class QueueSinkTestBase : RavenTestBase
    {
        private DocumentStore _src;

        protected static readonly BackupConfiguration DefaultBackupConfiguration;

        static QueueSinkTestBase()
        {
            var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
            configuration.Initialize();

            DefaultBackupConfiguration = configuration.Backup;
        }
        protected QueueSinkTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected AddQueueSinkOperationResult AddQueueSink<T>(DocumentStore src, QueueSinkConfiguration configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddQueueSinkOperation<T>(configuration));
            return addResult;
        }

        private async Task<string[]> GetQueueSinkErrorNotifications(DocumentStore src)
        {
            var databaseInstanceFor = await Databases.GetDocumentDatabaseInstanceFor(src);
            using (databaseInstanceFor.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                var notifications = storedNotifications
                    .Select(n => n.Json)
                    .Where(n => n.TryGet("AlertType", out string type) && type.StartsWith("QueueSink_"))
                    .Where(n => n.TryGet("Details", out BlittableJsonReaderObject _))
                    .Select(n =>
                    {
                        n.TryGet("Details", out BlittableJsonReaderObject details);
                        return details.ToString();
                    }).ToArray();
                return notifications;
            }
        }
        
        public override void Dispose()
        {
            try
            {
                if (_src == null)
                    return;

                if (Context.TestException == null || Context.TestOutput == null)
                    return;

                var notifications = GetQueueSinkErrorNotifications(_src).Result;
                if (notifications.Any() == false)
                    return;

                string message = string.Join(",\n", notifications);
                Context.TestOutput.WriteLine(message);
            }
            catch
            {
                // ignored
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}
