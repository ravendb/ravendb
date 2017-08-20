using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.ServerWide.Revisions;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Studio;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Replication;
using Raven.Server.Web.System;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Utils;
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
            scripter.AddType(typeof(SubscriptionCriteria));
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

            // cluster 
            scripter.AddType(typeof(ClusterTopology));

            // query 
            scripter.AddType(typeof(QueryResult<>));
            scripter.AddType(typeof(PutResult));

            // patch
            scripter.AddType(typeof(PatchRequest));
            scripter.AddType(typeof(PatchResult));
            scripter.AddType(typeof(PatchDebugActions));

            scripter.AddType(typeof(DatabasesInfo));

            // smuggler
            scripter.AddType(typeof(DatabaseSmugglerOptions));
            scripter.AddType(typeof(SmugglerResult));

            // revisions
            scripter.AddType(typeof(RevisionsConfiguration));

            // custom functions
            scripter.AddType(typeof(CustomFunctions));

            // storage report
            scripter.AddType(typeof(StorageReport));
            scripter.AddType(typeof(DetailedStorageReport));

            // map reduce visualizer
            scripter.AddType(typeof(ReduceTree));

            // license 
            scripter.AddType(typeof(License));
            scripter.AddType(typeof(UserRegistrationInfo));
            scripter.AddType(typeof(LicenseStatus));

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

            // ongoing tasks - common
            scripter.AddType(typeof(OngoingTasksResult));
            scripter.AddType(typeof(OngoingTask));
            scripter.AddType(typeof(OngoingTaskType));
            scripter.AddType(typeof(OngoingTaskState));
            scripter.AddType(typeof(OngoingTaskConnectionStatus));
            scripter.AddType(typeof(NodeId));
            scripter.AddType(typeof(ModifyOngoingTaskResult));

            // ongoing tasks - replication
            scripter.AddType(typeof(OngoingTaskReplication));
            scripter.AddType(typeof(ExternalReplication));

            // ongoing tasks - backup
            scripter.AddType(typeof(OngoingTaskBackup));
            scripter.AddType(typeof(PeriodicBackupConfiguration));
            scripter.AddType(typeof(PeriodicBackupTestConnectionType));
            scripter.AddType(typeof(RestoreBackupConfiguration));
            scripter.AddType(typeof(RestorePoints));
            scripter.AddType(typeof(NextBackupOccurrence));

            // ongoing tasks - subscription
            scripter.AddType(typeof(OngoingTaskSubscription));
            scripter.AddType(typeof(SubscriptionTryout));
            scripter.AddType(typeof(DocumentWithException));
            scripter.AddType(typeof(SubscriptionState));
            scripter.AddType(typeof(SubscriptionCriteria));
            scripter.AddType(typeof(ChangeVectorEntry));
            scripter.AddType(typeof(SubscriptionCreationOptions));

            // ongoing tasks - ravenDB ETL
            scripter.AddType(typeof(OngoingTaskRavenEtl));
            scripter.AddType(typeof(RavenEtlConfiguration));
            scripter.AddType(typeof(EtlProcessStatistics));
            scripter.AddType(typeof(EtlType));

            // ongoing tasks - SQL ETL
            scripter.AddType(typeof(OngoingTaskSqlEtl));
            scripter.AddType(typeof(SqlEtlConfiguration));
            scripter.AddType(typeof(SimulateSqlEtl));
            scripter.AddType(typeof(SqlEtlTable));

            // connection strings
            scripter.AddType(typeof(ConnectionString)); 
            scripter.AddType(typeof(RavenConnectionString)); 
            scripter.AddType(typeof(SqlConnectionString)); 
            scripter.AddType(typeof(ConnectionStringType)); 
            scripter.AddType(typeof(GetConnectionStringsResult)); 

            // adminJs console
            scripter.AddType(typeof(AdminJsScript));
            scripter.AddType(typeof(AdminJsScriptResult));

            scripter.AddType(typeof(NodeConnectionTestResult));

            return scripter;
        }
    }
}
