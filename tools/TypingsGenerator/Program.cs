using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Dashboard;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Providers.SQL.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Studio;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Raven.Server.Utils;
using Raven.Server.Utils.IoMetrics;
using Raven.Server.Web.Studio;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Meters;
using TypeScripter;
using TypeScripter.TypeScript;
using Voron.Data.BTrees;
using Voron.Debugging;
using LicenseConfiguration = Raven.Server.Config.Categories.LicenseConfiguration;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;
using Size = Sparrow.Size;

namespace TypingsGenerator
{
    public class Program
    {
        public const string TargetDirectory = "../../src/Raven.Studio/typings/server/";

        public static void Main(string[] args)
        {
            Directory.CreateDirectory(TargetDirectory);

            var scripter = new CustomScripter()
                .UsingFormatter(new TsFormatter
                {
                    EnumsAsString = true
                });

            scripter
                .WithTypeMapping(TsPrimitive.String, typeof(Char))
                .WithTypeMapping(TsPrimitive.String, typeof(Guid))
                .WithTypeMapping(TsPrimitive.String, typeof(TimeSpan))
                .WithTypeMapping(TsPrimitive.Number, typeof(Size))
                .WithTypeMapping(TsPrimitive.Number, typeof(UInt32))
                .WithTypeMapping(TsPrimitive.Number, typeof(UInt64))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(HashSet<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(List<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(IEnumerable<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(Queue<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(ConcurrentQueue<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(IReadOnlyList<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(IReadOnlyCollection<>))
                .WithTypeMapping(new TsInterface(new TsName("dictionary<Raven.Client.Documents.Queries.Timings.QueryTimings>")),
                    typeof(IDictionary<string, QueryTimings>))
                .WithTypeMapping(new TsInterface(new TsName("dictionary<Raven.Server.NotificationCenter.Notifications.Details.HugeDocumentInfo>")),
                    typeof(ConcurrentDictionary<string, HugeDocumentInfo>))
                .WithTypeMapping(TsPrimitive.Any, typeof(TreePage))
                .WithTypeMapping(TsPrimitive.String, typeof(DateTime))
                .WithTypeMapping(TsPrimitive.String, typeof(LazyStringValue))
                .WithTypeMapping(new TsArray(TsPrimitive.Any, 1), typeof(BlittableJsonReaderArray))
                .WithTypeMapping(new TsArray(TsPrimitive.Any, 1), typeof(DynamicJsonArray))
                .WithTypeMapping(new TsArray(TsPrimitive.Any, 1), typeof(IEnumerable))
                .WithTypeMapping(TsPrimitive.Any, typeof(TaskCompletionSource<object>))
                .WithTypeMapping(TsPrimitive.Any, typeof(BlittableJsonReaderObject));

            scripter = ConfigureTypes(scripter);
            Directory.Delete(TargetDirectory, true);
            Directory.CreateDirectory(TargetDirectory);
            scripter
                .SaveToDirectory(TargetDirectory);
        }

        private static Scripter ConfigureTypes(Scripter scripter)
        {
            var ignoredTypes = new HashSet<Type>
            {
                typeof(IEquatable<>),
                typeof(IComparable<>),
                typeof(IComparable),
                typeof(IConvertible),
                typeof(IDisposable),
                typeof(IFormattable),
                typeof(Exception)
            };

            scripter.UsingTypeFilter(type => ignoredTypes.Contains(type) == false);
            scripter.UsingTypeReader(new TypeReaderWithIgnoreMethods());

            scripter.AddType(typeof(CollectionStatistics));
            scripter.AddType(typeof(BatchRequestParser.CommandData));
            scripter.AddType(typeof(TransactionMode));

            // name validation
            scripter.AddType(typeof(StudioTasksHandler.ItemType));
            scripter.AddType(typeof(NameValidation));

            // database
            scripter.AddType(typeof(DatabasePutResult));
            scripter.AddType(typeof(DatabaseRecord));
            scripter.AddType(typeof(DatabaseStatistics));
            scripter.AddType(typeof(DetailedDatabaseStatistics));

            // database settings
            scripter.AddType(typeof(SettingsResult));
            scripter.AddType(typeof(ConfigurationEntryServerValue));
            scripter.AddType(typeof(ConfigurationEntryDatabaseValue));

            // restore database from cloud backup
            scripter.AddType(typeof(S3Settings));

            // footer
            scripter.AddType(typeof(FooterStatistics));
            scripter.AddType(typeof(IndexDefinition));
            scripter.AddType(typeof(PutIndexResult));
            scripter.AddType(typeof(IndexQuery));
            scripter.AddType(typeof(QueryTimings));
            scripter.AddType(typeof(DynamicQueryToIndexMatcher.Explanation));

            // attachments
            scripter.AddType(typeof(AttachmentName));
            scripter.AddType(typeof(AttachmentDetails));

            // notifications
            scripter.AddType(typeof(AlertRaised));
            scripter.AddType(typeof(NotificationUpdated));
            scripter.AddType(typeof(OperationChanged));
            scripter.AddType(typeof(BulkOperationResult.OperationDetails));
            scripter.AddType(typeof(DatabaseChanged));
            scripter.AddType(typeof(ClusterTopologyChanged));
            scripter.AddType(typeof(DatabaseStatsChanged));
            scripter.AddType(typeof(PerformanceHint));
            scripter.AddType(typeof(PagingPerformanceDetails));
            scripter.AddType(typeof(HugeDocumentsDetails));
            scripter.AddType(typeof(HugeDocumentInfo));
            scripter.AddType(typeof(RequestLatencyDetail));

            // subscriptions
            scripter.AddType(typeof(SubscriptionConnectionStats));
            scripter.AddType(typeof(SubscriptionWorkerOptions));
            scripter.AddType(typeof(SubscriptionTryout));

            // changes
            scripter.AddType(typeof(OperationStatusChange));
            scripter.AddType(typeof(DeterminateProgress));
            scripter.AddType(typeof(IndeterminateProgress));
            scripter.AddType(typeof(IndeterminateProgressCount));
            scripter.AddType(typeof(BulkOperationResult));
            scripter.AddType(typeof(ReplayTxOperationResult));
            scripter.AddType(typeof(BulkInsertProgress));
            scripter.AddType(typeof(OperationExceptionResult));
            scripter.AddType(typeof(DocumentChange));
            scripter.AddType(typeof(IndexChange));
            scripter.AddType(typeof(Operations.Operation));
            scripter.AddType(typeof(NewVersionAvailableDetails));
            scripter.AddType(typeof(MessageDetails));
            scripter.AddType(typeof(ExceptionDetails));

            // alerts
            scripter.AddType(typeof(EtlErrorsDetails));
            scripter.AddType(typeof(SlowSqlDetails));
            scripter.AddType(typeof(SlowWritesDetails));

            // indexes
            scripter.AddType(typeof(IndexDefinition));
            scripter.AddType(typeof(PutIndexResult));
            scripter.AddType(typeof(IndexStats));
            scripter.AddType(typeof(IndexingStatus));
            scripter.AddType(typeof(IndexPerformanceStats));
            scripter.AddType(typeof(IndexDefinition));
            scripter.AddType(typeof(TermsQueryResult));
            scripter.AddType(typeof(IndexProgress));
            scripter.AddType(typeof(IndexesProgress));
            scripter.AddType(typeof(IndexErrors));
            scripter.AddType(typeof(StudioTasksHandler.FormattedExpression));
            scripter.AddType(typeof(StudioIndexHandler.IndexTypeInfo));

            // cluster
            scripter.AddType(typeof(ClusterTopology));
            scripter.AddType(typeof(ClusterObserverDecisions));
            scripter.AddType(typeof(NodeInfo));

            // query
            scripter.AddType(typeof(QueryResult<,>));
            scripter.AddType(typeof(PutResult));

            // patch
            scripter.AddType(typeof(PatchRequest));
            scripter.AddType(typeof(PatchResult));
            scripter.AddType(typeof(PatchDebugActions));

            scripter.AddType(typeof(Raven.Client.ServerWide.Operations.DatabasesInfo));

            // smuggler
            scripter.AddType(typeof(DatabaseSmugglerImportOptions));
            scripter.AddType(typeof(DatabaseSmugglerOptionsServerSide));
            scripter.AddType(typeof(SmugglerResult));
            scripter.AddType(typeof(SingleDatabaseMigrationConfiguration));
            scripter.AddType(typeof(OfflineMigrationResult));
            scripter.AddType(typeof(OfflineMigrationProgress));
            scripter.AddType(typeof(BuildInfoWithResourceNames));
            scripter.AddType(typeof(MigratedServerUrls));
            scripter.AddType(typeof(MigrationConfiguration));
            scripter.AddType(typeof(CsvImportOptions));

            // revisions
            scripter.AddType(typeof(RevisionsConfiguration));
            scripter.AddType(typeof(RevertRevisionsRequest));
            scripter.AddType(typeof(RevertResult));
            scripter.AddType(typeof(EnforceConfigurationResult));
            scripter.AddType(typeof(GetRevisionsCountOperation.DocumentRevisionsCount));

            // server dashboard
            scripter.AddType(typeof(DashboardNotificationType));
            scripter.AddType(typeof(TrafficWatch));
            scripter.AddType(typeof(Raven.Server.Dashboard.DatabasesInfo));
            scripter.AddType(typeof(IndexingSpeed));
            scripter.AddType(typeof(MachineResources));
            scripter.AddType(typeof(DrivesUsage));

            // expiration
            scripter.AddType(typeof(ExpirationConfiguration));

            // documents compression
            scripter.AddType(typeof(DocumentsCompressionConfiguration));

            // refresh
            scripter.AddType(typeof(RefreshConfiguration));

            // storage report
            scripter.AddType(typeof(StorageReport));
            scripter.AddType(typeof(DetailedStorageReport));

            // map reduce visualizer
            scripter.AddType(typeof(ReduceTree));

            // license
            scripter.AddType(typeof(License));
            scripter.AddType(typeof(UserRegistrationInfo));
            scripter.AddType(typeof(LicenseStatus));
            scripter.AddType(typeof(LicenseLimitWarning));
            scripter.AddType(typeof(LicenseSupportInfo));
            scripter.AddType(typeof(LicenseRenewalResult));
            scripter.AddType(typeof(LicenseConfiguration));

            // feedback form
            scripter.AddType(typeof(FeedbackForm));

            // io metrics stats
            scripter.AddType(typeof(IOMetricsHistoryStats));
            scripter.AddType(typeof(IOMetricsRecentStats));
            scripter.AddType(typeof(IOMetricsRecentStatsAdditionalTypes));
            scripter.AddType(typeof(IOMetricsFileStats));
            scripter.AddType(typeof(IOMetricsEnvironment));
            scripter.AddType(typeof(IOMetricsResponse));
            scripter.AddType(typeof(FileStatus));
            scripter.AddType(typeof(IoMetrics.MeterType));

            // replication stats
            scripter.AddType(typeof(LiveReplicationPerformanceCollector.OutgoingPerformanceStats));
            scripter.AddType(typeof(LiveReplicationPerformanceCollector.IncomingPerformanceStats));

            // conflicts
            scripter.AddType(typeof(GetConflictsResult));
            scripter.AddType(typeof(ConflictResolverAdvisor.MergeResult));
            scripter.AddType(typeof(ConflictSolver));

            // ongoing tasks - common
            scripter.AddType(typeof(OngoingTasksResult));
            scripter.AddType(typeof(OngoingTask));
            scripter.AddType(typeof(OngoingTaskType));
            scripter.AddType(typeof(OngoingTaskState));
            scripter.AddType(typeof(OngoingTaskConnectionStatus));
            scripter.AddType(typeof(NodeId));
            scripter.AddType(typeof(ModifyOngoingTaskResult));
            scripter.AddType(typeof(Transformation));

            // ongoing tasks - replication
            scripter.AddType(typeof(OngoingTaskReplication));
            scripter.AddType(typeof(ExternalReplication));

            // ongoing tasks - pull replication
            scripter.AddType(typeof(PullReplicationDefinition));
            scripter.AddType(typeof(PullReplicationDefinitionAndCurrentConnections));
            scripter.AddType(typeof(PullReplicationAsSink));
            scripter.AddType(typeof(OngoingTaskPullReplicationAsSink));
            scripter.AddType(typeof(OngoingTaskPullReplicationAsHub));
            scripter.AddType(typeof(PullReplicationHandler.PullReplicationCertificate));
            scripter.AddType(typeof(ReplicationHubAccess));
            scripter.AddType(typeof(DetailedReplicationHubAccess));
            scripter.AddType(typeof(ReplicationHubAccessResult));

            // ongoing tasks - backup
            scripter.AddType(typeof(OngoingTaskBackup));
            scripter.AddType(typeof(PeriodicBackupConfiguration));
            scripter.AddType(typeof(PeriodicBackupConnectionType));
            scripter.AddType(typeof(RestoreBackupConfiguration));
            scripter.AddType(typeof(RestoreFromS3Configuration));
            scripter.AddType(typeof(RestoreFromAzureConfiguration));
            scripter.AddType(typeof(RestoreFromGoogleCloudConfiguration));
            scripter.AddType(typeof(RestoreType));
            scripter.AddType(typeof(RestorePoints));
            scripter.AddType(typeof(RestoreProgress));
            scripter.AddType(typeof(StudioTasksHandler.NextCronExpressionOccurrence));
            scripter.AddType(typeof(OfflineMigrationConfiguration));
            scripter.AddType(typeof(BackupProgress));
            scripter.AddType(typeof(StartBackupOperationResult));
            scripter.AddType(typeof(GetPeriodicBackupStatusOperationResult));

            // ongoing tasks - subscription
            scripter.AddType(typeof(OngoingTaskSubscription));
            scripter.AddType(typeof(SubscriptionTryout));
            scripter.AddType(typeof(DocumentWithException));
            scripter.AddType(typeof(SubscriptionStateWithNodeDetails));
            scripter.AddType(typeof(SubscriptionConnectionDetails));
            scripter.AddType(typeof(ChangeVectorEntry));
            scripter.AddType(typeof(SubscriptionCreationOptions));
            scripter.AddType(typeof(Constants.Documents.SubscriptionChangeVectorSpecialStates));
            scripter.AddType(typeof(SubscriptionOpeningStrategy));

            // ongoing tasks - ravenDB ETL
            scripter.AddType(typeof(EtlTaskProgress));
            scripter.AddType(typeof(OngoingTaskRavenEtlDetails));
            scripter.AddType(typeof(OngoingTaskRavenEtlListView));
            scripter.AddType(typeof(RavenEtlConfiguration));
            scripter.AddType(typeof(EtlProcessStatistics));
            scripter.AddType(typeof(TestRavenEtlScript));
            scripter.AddType(typeof(RavenEtlTestScriptResult));
            scripter.AddType(typeof(EtlType));
            scripter.AddType(typeof(EtlTaskPerformanceStats));

            // ongoing tasks - SQL ETL
            scripter.AddType(typeof(OngoingTaskSqlEtlDetails));
            scripter.AddType(typeof(OngoingTaskSqlEtlListView));
            scripter.AddType(typeof(SqlEtlConfiguration));
            scripter.AddType(typeof(TestSqlEtlScript));
            scripter.AddType(typeof(SqlEtlTable));
            scripter.AddType(typeof(SqlEtlTestScriptResult));

            // connection strings
            scripter.AddType(typeof(ConnectionString));
            scripter.AddType(typeof(RavenConnectionString));
            scripter.AddType(typeof(SqlConnectionString));
            scripter.AddType(typeof(ConnectionStringType));
            scripter.AddType(typeof(GetConnectionStringsResult));

            // server-wide tasks
            scripter.AddType(typeof(AdminStudioServerWideHandler.ServerWideTasksResult));
            scripter.AddType(typeof(AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask));
            scripter.AddType(typeof(AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideExternalReplicationTask));
            scripter.AddType(typeof(ServerWideBackupConfiguration));
            scripter.AddType(typeof(ServerWideExternalReplication));
            scripter.AddType(typeof(PutServerWideBackupConfigurationResponse));
            scripter.AddType(typeof(ServerWideExternalReplicationResponse));
            scripter.AddType(typeof(ServerWideTasksResult<>));

            // certificates
            scripter.AddType(typeof(CertificateDefinition));

            // admin logs
            scripter.AddType(typeof(LogMode));
            scripter.AddType(typeof(SetLogsConfigurationOperation.Parameters));

            // adminJs console
            scripter.AddType(typeof(AdminJsScript));

            scripter.AddType(typeof(TrafficWatchHttpChange));
            scripter.AddType(typeof(TrafficWatchTcpChange));

            scripter.AddType(typeof(NodeConnectionTestResult));
            scripter.AddType(typeof(ClientCertificateGenerationResult));

            // request with POST parameters
            scripter.AddType(typeof(DeleteDatabasesOperation.Parameters));
            scripter.AddType(typeof(ToggleDatabasesStateOperation.Parameters));
            scripter.AddType(typeof(SetIndexesLockOperation.Parameters));
            scripter.AddType(typeof(SetIndexesPriorityOperation.Parameters));
            scripter.AddType(typeof(AdminRevisionsHandler.Parameters));
            scripter.AddType(typeof(ReorderDatabaseMembersOperation.Parameters));

            scripter.AddType(typeof(LicenseLimitException));

            scripter.AddType(typeof(CompactSettings));
            scripter.AddType(typeof(CompactionResult));
            scripter.AddType(typeof(CompactionProgress));

            // server setup
            scripter.AddType(typeof(UnsecuredSetupInfo));
            scripter.AddType(typeof(SetupInfo));
            scripter.AddType(typeof(ContinueSetupInfo));
            scripter.AddType(typeof(ClaimDomainInfo));
            scripter.AddType(typeof(SetupProgressAndResult));
            scripter.AddType(typeof(UserDomainsAndLicenseInfo));
            scripter.AddType(typeof(UserDomainsWithIps));
            scripter.AddType(typeof(SetupMode));
            scripter.AddType(typeof(ConfigurationNodeInfo));
            scripter.AddType(typeof(SetupParameters));

            // compare exchange
            scripter.AddType(typeof(CompareExchangeHandler.CompareExchangeListItem));
            scripter.AddType(typeof(CompareExchangeResult<object>));
            scripter.AddType(typeof(CompareExchangeValue<object>));

            // debug
            scripter.AddType(typeof(ThreadsInfo));
            scripter.AddType(typeof(MemoryDebugHandler.MemoryInfo));

            // counters
            scripter.AddType(typeof(CounterBatch));
            scripter.AddType(typeof(CountersDetail));
            scripter.AddType(typeof(CounterDetail));
            scripter.AddType(typeof(CounterOperationType));

            // sql migration
            scripter.AddType(typeof(DatabaseSchema));
            scripter.AddType(typeof(MigrationProvider));
            scripter.AddType(typeof(MigrationRequest));
            scripter.AddType(typeof(MigrationResult));
            scripter.AddType(typeof(MigrationProgress));
            scripter.AddType(typeof(MigrationTestRequest));

            // document size details
            scripter.AddType(typeof(DocumentSizeDetails));

            // version info
            scripter.AddType(typeof(LatestVersionCheck.VersionInfo));

            // time series
            scripter.AddType(typeof(TimeSeriesStatistics));
            scripter.AddType(typeof(TimeSeriesDetails));
            scripter.AddType(typeof(TimeSeriesOperation));
            scripter.AddType(typeof(TimeSeriesOperation.AppendOperation));
            scripter.AddType(typeof(TimeSeriesOperation.DeleteOperation));
            scripter.AddType(typeof(TimeSeriesConfiguration));

            // studio configuration
            scripter.AddType(typeof(ServerWideStudioConfiguration));
            scripter.AddType(typeof(StudioConfiguration));

            scripter.AddType(typeof(StudioTasksHandler.OfflineMigrationValidation));

            scripter.AddType(typeof(StartTransactionsRecordingOperation.Parameters));
            scripter.AddType(typeof(TransactionsRecordingHandler.RecordingDetails));

            scripter.AddType(typeof(FolderPathOptions));
            scripter.AddType(typeof(DataDirectoryResult));

            scripter.AddType(typeof(LiveRunningQueriesCollector.ExecutingQueryCollection));
            scripter.AddType(typeof(SorterDefinition));
            scripter.AddType(typeof(AdminIndexHandler.DumpIndexResult));

            return scripter;
        }
    }
}
