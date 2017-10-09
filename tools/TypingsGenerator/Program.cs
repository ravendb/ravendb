using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Expiration;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.ServerWide.Revisions;
using Raven.Server.Commercial;
using Raven.Server.Dashboard;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Studio;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Web.System;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Raven.Server.Web.Studio;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using TypeScripter;
using TypeScripter.TypeScript;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;
using Voron.Data.BTrees;
using Voron.Debugging;

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
                .WithTypeMapping(TsPrimitive.String, typeof(Guid))
                .WithTypeMapping(TsPrimitive.String, typeof(TimeSpan))
                .WithTypeMapping(TsPrimitive.Number, typeof(UInt32))
                .WithTypeMapping(TsPrimitive.Number, typeof(UInt64))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(HashSet<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(List<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(IEnumerable<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(Queue<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(IReadOnlyList<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(IReadOnlyCollection<>))
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

            scripter.AddType(typeof(DatabasePutResult));
            scripter.AddType(typeof(DatabaseRecord));
            scripter.AddType(typeof(DatabaseStatistics));
            scripter.AddType(typeof(FooterStatistics));
            scripter.AddType(typeof(IndexDefinition));
            scripter.AddType(typeof(PutIndexResult));

            // attachments
            scripter.AddType(typeof(AttachmentName));
            scripter.AddType(typeof(AttachmentDetails));

            // notifications
            scripter.AddType(typeof(AlertRaised));
            scripter.AddType(typeof(NotificationUpdated));
            scripter.AddType(typeof(OperationChanged));
            scripter.AddType(typeof(DatabaseChanged));
            scripter.AddType(typeof(ClusterTopologyChanged));
            scripter.AddType(typeof(DatabaseStatsChanged));
            scripter.AddType(typeof(PerformanceHint));
            scripter.AddType(typeof(PagingPerformanceDetails));

            // subscriptions
            scripter.AddType(typeof(SubscriptionConnectionStats));
            scripter.AddType(typeof(SubscriptionConnectionOptions));
            scripter.AddType(typeof(SubscriptionTryout));

            // changes
            scripter.AddType(typeof(OperationStatusChange));
            scripter.AddType(typeof(DeterminateProgress));
            scripter.AddType(typeof(IndeterminateProgress));
            scripter.AddType(typeof(BulkOperationResult));
            scripter.AddType(typeof(IndexCompactionProgress));
            scripter.AddType(typeof(IndexCompactionResult));
            scripter.AddType(typeof(BulkInsertProgress));
            scripter.AddType(typeof(OperationExceptionResult));
            scripter.AddType(typeof(DocumentChange));
            scripter.AddType(typeof(IndexChange));
            scripter.AddType(typeof(Operations.Operation));
            scripter.AddType(typeof(NewVersionAvailableDetails));
            scripter.AddType(typeof(MessageDetails));
            scripter.AddType(typeof(ExceptionDetails));

            // indexes
            scripter.AddType(typeof(IndexStats));
            scripter.AddType(typeof(IndexingStatus));
            scripter.AddType(typeof(IndexPerformanceStats));
            scripter.AddType(typeof(IndexDefinition));
            scripter.AddType(typeof(TermsQueryResult));
            scripter.AddType(typeof(IndexProgress));
            scripter.AddType(typeof(IndexErrors));
            scripter.AddType(typeof(StudioTasksHandler.FormatedExpression));

            // cluster 
            scripter.AddType(typeof(ClusterTopology));
            scripter.AddType(typeof(ClusterObserverDecisions));

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
            scripter.AddType(typeof(DatabaseSmugglerExportOptions));
            scripter.AddType(typeof(SmugglerResult));
            scripter.AddType(typeof(SingleDatabaseMigrationConfiguration));
            scripter.AddType(typeof(OfflineMigrationResult));
            scripter.AddType(typeof(OfflineMigrationProgress));
            scripter.AddType(typeof(BuildInfo));

            // revisions
            scripter.AddType(typeof(RevisionsConfiguration));
            
            // server dashboard
            scripter.AddType(typeof(DashboardNotificationType));
            scripter.AddType(typeof(TrafficWatch));
            scripter.AddType(typeof(Raven.Server.Dashboard.DatabasesInfo));
            scripter.AddType(typeof(IndexingSpeed));
            
            // expiration 
            scripter.AddType(typeof(ExpirationConfiguration));

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
            
            // database compact
            scripter.AddType(typeof(DatabaseCompactionResult));
            scripter.AddType(typeof(DatabaseCompactionProgress));

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

            // ongoing tasks - backup
            scripter.AddType(typeof(OngoingTaskBackup));
            scripter.AddType(typeof(PeriodicBackupConfiguration));
            scripter.AddType(typeof(PeriodicBackupTestConnectionType));
            scripter.AddType(typeof(RestoreBackupConfiguration));
            scripter.AddType(typeof(RestorePoints));
            scripter.AddType(typeof(RestoreProgress));
            scripter.AddType(typeof(NextBackupOccurrence));
            scripter.AddType(typeof(OfflineMigrationConfiguration));

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
            scripter.AddType(typeof(OngoingTaskRavenEtlDetails));
            scripter.AddType(typeof(OngoingTaskRavenEtlListView));
            scripter.AddType(typeof(RavenEtlConfiguration));
            scripter.AddType(typeof(EtlProcessStatistics));
            scripter.AddType(typeof(EtlType));

            // ongoing tasks - SQL ETL
            scripter.AddType(typeof(OngoingTaskSqlEtlDetails));
            scripter.AddType(typeof(OngoingTaskSqlEtlListView));
            scripter.AddType(typeof(SqlEtlConfiguration));
            scripter.AddType(typeof(SimulateSqlEtl));
            scripter.AddType(typeof(SqlEtlTable));

            // connection strings
            scripter.AddType(typeof(ConnectionString));
            scripter.AddType(typeof(RavenConnectionString));
            scripter.AddType(typeof(SqlConnectionString));
            scripter.AddType(typeof(ConnectionStringType));
            scripter.AddType(typeof(GetConnectionStringsResult));

            // certificates
            scripter.AddType(typeof(CertificateDefinition));

            // adminJs console
            scripter.AddType(typeof(AdminJsScript));
            
            scripter.AddType(typeof(TrafficWatchChange));

            scripter.AddType(typeof(NodeConnectionTestResult));
            scripter.AddType(typeof(ClientCertificateGenerationResult));
            
            // request with POST parameters
            scripter.AddType(typeof(DeleteDatabasesOperation.Parameters));
            scripter.AddType(typeof(ToggleDatabasesStateOperation.Parameters));
            scripter.AddType(typeof(SetIndexesLockOperation.Parameters));
            scripter.AddType(typeof(SetIndexesPriorityOperation.Parameters));
            scripter.AddType(typeof(AdminRevisionsHandler.Parameters));
            scripter.AddType(typeof(ReorderDatabaseMembersOperation.Parameters));
            
            scripter.AddType(typeof(LicenseLimit));

            return scripter;
        }
    }
}
