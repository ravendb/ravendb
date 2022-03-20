using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Tests.Infrastructure.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    [Trait("Category", "ETL")]
    public abstract class EtlTestBase : RavenTestBase
    {
        private DocumentStore _src;

        protected static readonly BackupConfiguration DefaultBackupConfiguration;

        static EtlTestBase()
        {
            var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
            configuration.Initialize();

            DefaultBackupConfiguration = configuration.Backup;
        }
        protected EtlTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected AddEtlOperationResult AddEtl<T>(DocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
            return addResult;
        }

        protected AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            return AddEtl(src, dst, new[] { collection }, script, applyToAllDocuments, disabled, mentor);
        }

        protected AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

            return AddEtl(src, new RavenEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(collections),
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments,
                            Disabled = disabled
                        }
                    },
                MentorNode = mentor,
            },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    TopologyDiscoveryUrls = dst.Urls,
                }
            );
        }

        protected (DocumentStore src, DocumentStore dest, AddEtlOperationResult result) CreateSrcDestAndAddEtl(string collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null, Options srcOptions = null)
        {
            return CreateSrcDestAndAddEtl(new[] { collections }, script, applyToAllDocuments, disabled, mentor, srcOptions);
        }

        protected (DocumentStore src, DocumentStore dest, AddEtlOperationResult result) CreateSrcDestAndAddEtl(IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null, Options srcOptions = null)
        {
            _src = GetDocumentStore(srcOptions);
            var dest = GetDocumentStore();

            var result = AddEtl(_src, dest, collections, script, applyToAllDocuments);
            return (_src, dest, result);
        }

        protected ManualResetEventSlim WaitForEtl(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };


            return mre;
        }

        protected async Task<(string, string, EtlProcessStatistics)> WaitForEtlAsync(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate, TimeSpan timeout)
        {
            var database = GetDatabase(store.Database).Result;

            var taskCompletionSource = new TaskCompletionSource<(string, string, EtlProcessStatistics)>();

            void EtlLoaderOnBatchCompleted((string ConfigurationName, string TransformationName, EtlProcessStatistics Statistics) x)
            {
                try
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics) == false)
                        return;
                    taskCompletionSource.SetResult(x);
                }
                catch (Exception e)
                {
                    taskCompletionSource.SetException(e);
                }
            }

            database.EtlLoader.BatchCompleted += EtlLoaderOnBatchCompleted;
            var whenAny = await Task.WhenAny(taskCompletionSource.Task, Task.Delay(timeout));
            database.EtlLoader.BatchCompleted -= EtlLoaderOnBatchCompleted;

            if (whenAny != taskCompletionSource.Task)
                throw new TimeoutException($"Etl predicate timeout - {timeout}");

            return await taskCompletionSource.Task;
        }

        private async Task<string[]> GetEtlErrorNotifications(DocumentStore src)
        {
            var databaseInstanceFor = await Databases.GetDocumentDatabaseInstanceFor(src);
            using (databaseInstanceFor.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                var notifications = storedNotifications
                    .Select(n => n.Json)
                    .Where(n => n.TryGet("AlertType", out string type) && type.StartsWith("Etl_"))
                    .Where(n => n.TryGet("Details", out BlittableJsonReaderObject _))
                    .Select(n =>
                    {
                        n.TryGet("Details", out BlittableJsonReaderObject details);
                        return details.ToString();
                    }).ToArray();
                return notifications;
            }
        }

        protected IAsyncDisposable OpenEtlOffArea(IDocumentStore store, long etlTaskId, bool cleanTombstones = false)
        {
            store.Maintenance.Send(new ToggleOngoingTaskStateOperation(etlTaskId, OngoingTaskType.RavenEtl, true));
            return new DisposableAsyncAction(async () =>
            {
                if (cleanTombstones)
                {
                    var srcDatabase = await GetDatabase(store.Database);
                    await srcDatabase.TombstoneCleaner.ExecuteCleanup();
                }

                store.Maintenance.Send(new ToggleOngoingTaskStateOperation(etlTaskId, OngoingTaskType.RavenEtl, false));
            });
        }

        protected bool TryGetLoadError<T>(string databaseName, EtlConfiguration<T> config, out EtlErrorInfo error) where T : ConnectionString
        {
            var database = GetDatabase(databaseName).Result;

            string tag;

            if (typeof(T) == typeof(ElasticSearchConnectionString))
                tag = ElasticSearchEtl.ElasticSearchEtlTag;
            else if (typeof(T) == typeof(SqlConnectionString))
                tag = SqlEtl.SqlEtlTag;
            else if (typeof(T) == typeof(RavenConnectionString))
                tag = RavenEtl.RavenEtlTag;
            else if (typeof(T) == typeof(OlapConnectionString))
                tag = OlapEtl.OlaptEtlTag;
            else
                throw new NotSupportedException($"Unknown ETL type: {typeof(T)}");

            var loadAlert = database.NotificationCenter.EtlNotifications.GetAlert<EtlErrorsDetails>(tag, $"{config.Name}/{config.Transforms.First().Name}", AlertType.Etl_LoadError);

            if (loadAlert.Errors.Count != 0)
            {
                error = loadAlert.Errors.First();

                return true;
            }

            error = null;
            return false;
        }

        protected bool TryGetTransformationError<T>(string databaseName, EtlConfiguration<T> config, out EtlErrorInfo error) where T : ConnectionString
        {
            var database = GetDatabase(databaseName).Result;

            string tag;

            if (typeof(T) == typeof(ElasticSearchConnectionString))
                tag = ElasticSearchEtl.ElasticSearchEtlTag;
            else if (typeof(T) == typeof(SqlConnectionString))
                tag = SqlEtl.SqlEtlTag;
            else if (typeof(T) == typeof(RavenConnectionString))
                tag = RavenEtl.RavenEtlTag;
            else if (typeof(T) == typeof(OlapConnectionString))
                tag = OlapEtl.OlaptEtlTag;
            else
                throw new NotSupportedException($"Unknown ETL type: {typeof(T)}");

            var loadAlert = database.NotificationCenter.EtlNotifications.GetAlert<EtlErrorsDetails>(tag, $"{config.Name}/{config.Transforms.First().Name}", AlertType.Etl_TransformationError);

            if (loadAlert.Errors.Count != 0)
            {
                error = loadAlert.Errors.First();

                return true;
            }

            error = null;
            return false;
        }

        public override void Dispose()
        {
            try
            {
                if (_src == null)
                    return;

                if (Context.TestException == null || Context.TestOutput == null)
                    return;

                var notifications = GetEtlErrorNotifications(_src).Result;
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
