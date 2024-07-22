using System;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.ETL.Providers.Queue;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Tests.Infrastructure.ConnectionString;
using Tests.Infrastructure.Utils;
using Xunit;
using Raven.Server.Documents;
using System.Text;
using Newtonsoft.Json;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents.ETL.Providers.Snowflake;
using Tests.Infrastructure;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        public readonly EtlTestBase Etl;

        public class EtlTestBase
        {
            private readonly RavenTestBase _parent;

            public EtlTestBase(RavenTestBase parent)
            {
                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            }

            private DocumentStore _src;
            private DocumentStore _dest;

            public static readonly BackupConfiguration DefaultBackupConfiguration;

            static EtlTestBase()
            {
                var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
                configuration.Initialize();

                DefaultBackupConfiguration = configuration.Backup;
            }

            public AddEtlOperationResult AddEtl<T>(IDocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
            {
                var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
                Assert.NotNull(putResult.RaftCommandIndex);

                var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
                return addResult;
            }

            public AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null, bool pinToMentorNode = false)
            {
                return AddEtl(src, dst, new[] { collection }, script, applyToAllDocuments, disabled, mentor, pinToMentorNode);
            }

            public AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null, bool pinToMentorNode = false)
            {
                return AddEtl(src, dst, collections, script, applyToAllDocuments, disabled, mentor, pinToMentorNode, out _);
            }

            public AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, out RavenEtlConfiguration configuration)
            {
                return AddEtl(src, dst, collections, script, false, false, null, false, out configuration);
            }

            public AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, bool applyToAllDocuments, bool disabled, string mentor, bool pinToMentorNode, out RavenEtlConfiguration configuration)
            {
                var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

                var transformation = new Transformation
                {
                    Name = $"ETL : {connectionStringName}",
                    Collections = new List<string>(collections),
                    Script = script,
                    ApplyToAllDocuments = applyToAllDocuments,
                    Disabled = disabled
                };

                configuration = new RavenEtlConfiguration
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                {
                    transformation
                },
                    MentorNode = mentor,
                    PinToMentorNode = pinToMentorNode
                };

                return AddEtl(src, configuration,
                    new RavenConnectionString
                    {
                        Name = connectionStringName,
                        Database = dst.Database,
                        TopologyDiscoveryUrls = dst.Urls,
                    }
                );
            }

            public (DocumentStore src, DocumentStore dest, AddEtlOperationResult result) CreateSrcDestAndAddEtl(string collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null, Options srcOptions = null, [CallerMemberName] string caller = null)
            {
                return CreateSrcDestAndAddEtl(new[] { collections }, script, applyToAllDocuments, disabled, mentor, srcOptions, caller);
            }

            public (DocumentStore src, DocumentStore dest, AddEtlOperationResult result) CreateSrcDestAndAddEtl(IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null, Options srcOptions = null, [CallerMemberName] string caller = null)
            {
                _src = _parent.GetDocumentStore(srcOptions, caller);
                _dest = _parent.GetDocumentStore(caller: caller);

                var result = AddEtl(_src, _dest, collections, script, applyToAllDocuments);
                return (_src, _dest, result);
            }

            public ManualResetEventSlim WaitForEtlToComplete(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate = null, int numOfProcessesToWaitFor = 1)
            {
                predicate ??= (n, statistics) => statistics.LoadSuccesses > 0;
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                return record.IsSharded
                    ? AsyncHelpers.RunSync(() => _parent.Sharding.Etl.WaitForEtlAsync(store, predicate, numOfProcessesToWaitFor))
                    : WaitForEtl(store, predicate);
            }

            public Task AssertEtlReachedDestination(Action act, int timeout = 30000, int interval = 100)
            {
                return AssertWaitForTrueAsync(() =>
                {
                    act.Invoke();
                    return Task.FromResult(true);
                }, timeout, interval);
            }

            private ManualResetEventSlim WaitForEtl(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
            {
                var database = AsyncHelpers.RunSync(() => _parent.GetDatabase(store.Database));

                var mre = new ManualResetEventSlim();

                database.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                        mre.Set();
                };


                return mre;
            }

            public async Task<(string, string, EtlProcessStatistics)> WaitForEtlAsync(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate, TimeSpan timeout)
            {
                var database = await _parent.GetDatabase(store.Database);

                var taskCompletionSource = new TaskCompletionSource<(string, string, EtlProcessStatistics)>(TaskCreationOptions.RunContinuationsAsynchronously);

                void EtlLoaderOnBatchCompleted((string ConfigurationName, string TransformationName, EtlProcessStatistics Statistics) x)
                {
                    try
                    {
                        if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics) == false)
                            return;
                        taskCompletionSource.TrySetResult(x);
                    }
                    catch (Exception e)
                    {
                        taskCompletionSource.TrySetException(e);
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
                var databaseInstanceFor = await _parent.Databases.GetDocumentDatabaseInstanceFor(src);
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

            public IAsyncDisposable OpenEtlOffArea(IDocumentStore store, long etlTaskId, bool cleanTombstones = false)
            {
                store.Maintenance.Send(new ToggleOngoingTaskStateOperation(etlTaskId, OngoingTaskType.RavenEtl, true));
                return new DisposableAsyncAction(async () =>
                {
                    if (cleanTombstones)
                    {
                        var srcDatabase = await _parent.GetDatabase(store.Database);
                        await srcDatabase.TombstoneCleaner.ExecuteCleanup();
                    }

                    store.Maintenance.Send(new ToggleOngoingTaskStateOperation(etlTaskId, OngoingTaskType.RavenEtl, false));
                });
            }

            public bool TryGetLoadError<T>(string databaseName, EtlConfiguration<T> config, out EtlErrorInfo error) where T : ConnectionString
            {
                var database = _parent.GetDatabase(databaseName).Result;

                string tag;

                if (typeof(T) == typeof(ElasticSearchConnectionString))
                    tag = ElasticSearchEtl.ElasticSearchEtlTag;
                else if (typeof(T) == typeof(SqlConnectionString<>))
                    tag = SqlEtl.SqlEtlTag;
                else if (typeof(T) == typeof(RavenConnectionString))
                    tag = RavenEtl.RavenEtlTag;
                else if (typeof(T) == typeof(OlapConnectionString))
                    tag = OlapEtl.OlaptEtlTag;
                else if (typeof(T) == typeof(QueueConnectionString))
                    tag = QueueEtl<QueueItem>.QueueEtlTag;
                else if (typeof(T) == typeof(SnowflakeConnectionString))
                    tag = SnowflakeEtl.SnowflakeEtlTag;
                else
                    throw new NotSupportedException($"Unknown ETL type: {typeof(T)}");

                var loadAlert = database.NotificationCenter.EtlNotifications.GetAlert<EtlErrorsDetails>(tag, $"{config.Name}/{config.Transforms.First().Name}", AlertType.Etl_LoadError);

                if (loadAlert is null)
                {
                    error = null;
                    return false;
                }

                var details = (EtlErrorsDetails)loadAlert.Details;

                if (details.Errors.Count != 0)
                {
                    error = details.Errors.First();

                    return true;
                }

                error = null;
                return false;
            }

            public bool TryGetTransformationError<T>(string databaseName, EtlConfiguration<T> config, out EtlErrorInfo error) where T : ConnectionString
            {
                var database = _parent.GetDatabase(databaseName).Result;

                string tag;

                if (typeof(T) == typeof(ElasticSearchConnectionString))
                    tag = ElasticSearchEtl.ElasticSearchEtlTag;
                else if (typeof(T) == typeof(SqlConnectionString))
                    tag = SqlEtl.SqlEtlTag;
                else if (typeof(T) == typeof(RavenConnectionString))
                    tag = RavenEtl.RavenEtlTag;
                else if (typeof(T) == typeof(OlapConnectionString))
                    tag = OlapEtl.OlaptEtlTag;
                else if (typeof(T) == typeof(QueueConnectionString))
                    tag = QueueEtl<QueueItem>.QueueEtlTag;
                else if (typeof(T) == typeof(SnowflakeConnectionString))
                    tag = SnowflakeEtl.SnowflakeEtlTag;
                else
                    throw new NotSupportedException($"Unknown ETL type: {typeof(T)}");

                var loadAlert = database.NotificationCenter.EtlNotifications.GetAlert<EtlErrorsDetails>(tag, $"{config.Name}/{config.Transforms.First().Name}", AlertType.Etl_TransformationError);
                
                if (loadAlert is null)
                {
                    error = null;
                    return false;
                }

                var details = (EtlErrorsDetails)loadAlert.Details;

                if (details.Errors.Count != 0)
                {
                    error = details.Errors.First();

                    return true;
                }

                error = null;
                return false;
            }

            public async Task<DocumentDatabase> GetDatabaseFor(IDocumentStore store, string docId)
            {
                var databaseName = store.Database;
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName));
                if (record.IsSharded)
                    databaseName = await _parent.Sharding.GetShardDatabaseNameForDocAsync(store, docId);
                return await _parent.GetDocumentDatabaseInstanceFor(store, databaseName);
            }

            public async Task<string> GetEtlDebugInfo(string database, TimeSpan? timeout = null, RavenDatabaseMode databaseMode = RavenDatabaseMode.Single, RavenServer server = null)
            {
                IEnumerable<DocumentDatabase> databases = databaseMode switch
                {
                    RavenDatabaseMode.Single => new[] { await GetDatabase(server ?? _parent.Server, database) },
                    RavenDatabaseMode.Sharded => await _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(database, server == null ? null : [server]).ToListAsync(),
                    _ => throw new ArgumentOutOfRangeException(nameof(databaseMode), databaseMode, null)
                };

                var sb = new StringBuilder().AppendLine($"ETL did not finish in {timeout?.TotalSeconds ?? 30} seconds.");

                foreach (var documentDatabase in databases)
                {
                    var performanceStats = GetEtlPerformanceStatsForDatabase(documentDatabase);
                    if (performanceStats == null)
                        continue;
                    sb.AppendLine($"Database '{documentDatabase.Name}' ETL performance stats : {performanceStats}");
                }

                return sb.ToString();
            }

            public string GetEtlPerformanceStatsForDatabase(DocumentDatabase database)
            {
                var process = database.EtlLoader.Processes.FirstOrDefault();
                if (process == null)
                    return null;
                var stats = process.GetPerformanceStats();
                return string.Join(Environment.NewLine, stats.Select(JsonConvert.SerializeObject));
            }

            public void Dispose()
            {
                try
                {
                    using (_src)
                    using (_dest)
                    {

                        if (_src == null)
                            return;

                        if (_parent.Context.TestException == null || _parent.Context.TestOutput == null)
                            return;

                        var notifications = GetEtlErrorNotifications(_src).Result;
                        if (notifications.Any() == false)
                            return;

                        string message = string.Join(",\n", notifications);
                        _parent.Context.TestOutput.WriteLine(message);
                    }
                }
                catch
                {
                    // ignored
                }
            }
        }
    }
}
