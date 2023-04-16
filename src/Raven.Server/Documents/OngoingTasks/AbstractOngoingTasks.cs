using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.OngoingTasks;

public abstract class AbstractOngoingTasks
{
    protected abstract IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskBackup> CollectBackupTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskRavenEtlListView> CollectRavenEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskSqlEtlListView> CollectSqlEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskOlapEtlListView> CollectOlapEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskElasticSearchEtlListView> CollectElasticEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskQueueEtlListView> CollectQueueEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskPullReplicationAsHub> CollectPullReplicationAsHubTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);

    public IEnumerable<OngoingTask> GetAllTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var task in CollectSubscriptionTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectBackupTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectRavenEtlTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectSqlEtlTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectOlapEtlTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectElasticEtlTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectQueueEtlTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectPullReplicationAsSinkTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectPullReplicationAsHubTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectExternalReplicationTasks(context, clusterTopology, databaseRecord))
            yield return task;
    }

    public OngoingTask GetTask(TransactionOperationContext context, long? taskId, string taskName, OngoingTaskType taskType, ClusterTopology clusterTopology, DatabaseRecord record)
    {

    }

    protected abstract OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
        where T : ConnectionString;

    protected abstract (string Url, OngoingTaskConnectionStatus Status) GetReplicationTaskConnectionStatus<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, T replication, Dictionary<string, RavenConnectionString> connectionStrings, out string tag, out RavenConnectionString connection)
        where T : ExternalReplicationBase;

    protected abstract PeriodicBackupStatus GetBackupStatus(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, out string responsibleNodeTag, out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted);
}
