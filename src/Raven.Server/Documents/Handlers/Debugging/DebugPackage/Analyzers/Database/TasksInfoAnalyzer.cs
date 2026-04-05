using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Dashboard;
using Raven.Server.Documents.ETL.Handlers;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class TasksInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private DateTime? _debugInfoCollectedAt;
    public TasksAnalysisInfo TasksInfo { get; set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetEntry<OngoingTasksHandler>(x => x.GetOngoingTasks(), out var ongoingTasksEntry) == false)
        {
            AddWarning("Failed to get ongoing tasks");
        }

        if (entries.TryGetValue<OngoingTasksHandler, OngoingTasksResult>(x => x.GetOngoingTasks(), out var ongoingTasks) == false)
        {
            AddWarning("Failed to get ongoing tasks");
        }

        if (entries.TryGetValue<SubscriptionsHandler, List<SubscriptionState>>(x => x.GetAll(), "Results", out var subscriptionStates) == false)
        {
            AddWarning("Failed to get subscriptions");
        }

        if (entries.TryGetValue<EtlHandler, List<EtlTaskStats>>(x => x.Stats(), "Results", out var etlStats) == false)
        {
            AddWarning("Failed to get ETL task stats");
        }

        if (entries.TryGetValue<EtlHandler, List<EtlTaskProgress>>(x => x.Progress(), "Results", out var etlProgress) == false)
        {
            AddWarning("Failed to get progress of ETL tasks");
        }

        if (ongoingTasksEntry.TryGetJson("@metadata", out JsonElement metadata) && metadata.TryGetProperty("DateTime", out var dateTimeField) &&
            dateTimeField.TryGetDateTime(out var retrievalInfoDateTime))
        {
            _debugInfoCollectedAt = retrievalInfoDateTime;
        }

        var taskCounts = new DatabaseOngoingTasksInfoItem();

        TasksInfo = new TasksAnalysisInfo { TaskCounts = taskCounts };

        foreach (var ongoingTask in ongoingTasks.OngoingTasks)
        {
            taskCounts.Total++;

            switch (ongoingTask.TaskType)
            {
                case OngoingTaskType.Backup:
                    taskCounts.PeriodicBackupCount++;

                    TasksInfo.BackupTasks ??= new List<OngoingTaskBackup>();
                    TasksInfo.BackupTasks.Add((OngoingTaskBackup)ongoingTask);
                    break;
                case OngoingTaskType.Replication:
                    taskCounts.ExternalReplicationCount++;
                    break;
                case OngoingTaskType.RavenEtl:
                    taskCounts.RavenEtlCount++;
                    break;
                case OngoingTaskType.SqlEtl:
                    taskCounts.SqlEtlCount++;
                    break;
                case OngoingTaskType.OlapEtl:
                    taskCounts.OlapEtlCount++;
                    break;
                case OngoingTaskType.ElasticSearchEtl:
                    taskCounts.ElasticSearchEtlCount++;
                    break;
                case OngoingTaskType.QueueEtl:
                    if (ongoingTask is OngoingTaskQueueEtl queueEtl)
                    {
                        if (queueEtl.BrokerType == QueueBrokerType.AzureQueueStorage)
                            taskCounts.AzureQueueStorageEtlCount++;
                        else if (queueEtl.BrokerType == QueueBrokerType.Kafka)
                            taskCounts.KafkaEtlCount++;
                        else if (queueEtl.BrokerType == QueueBrokerType.RabbitMq)
                            taskCounts.RabbitMqEtlCount++;
                        else
                            throw new NotSupportedException($"Unknown queue broker type: {queueEtl.BrokerType}");
                    }

                    break;
                case OngoingTaskType.QueueSink:
                    if (ongoingTask is OngoingTaskQueueSink queueSink)
                    {
                        if (queueSink.BrokerType == QueueBrokerType.Kafka)
                            taskCounts.KafkaSinkCount++;
                        else if (queueSink.BrokerType == QueueBrokerType.RabbitMq)
                            taskCounts.RabbitMqSinkCount++;
                        else
                            throw new NotSupportedException($"Unknown queue broker type: {queueSink.BrokerType}");
                    }

                    break;
                case OngoingTaskType.PullReplicationAsHub:
                    // hub replications aren't listed here - there is ongoingTasks.PullReplications
                    break;
                case OngoingTaskType.PullReplicationAsSink:
                    taskCounts.ReplicationSinkCount++;
                    break;
                case OngoingTaskType.Subscription:
                    taskCounts.SubscriptionCount++;
                    break;
                case OngoingTaskType.SnowflakeEtl:
                    taskCounts.SnowflakeEtlCount++;
                    break;
                case OngoingTaskType.EmbeddingsGeneration:
                    taskCounts.EmbeddingsGenerationCount++;
                    break;
                case OngoingTaskType.GenAi:
                    taskCounts.GenAiCount++;
                    break;
                case OngoingTaskType.CdcSink:
                    taskCounts.CdcSinkCount++;
                    break;
                default:
                    throw new NotSupportedException($"Unknown task type: {ongoingTask.TaskType}");
            }
        }

        taskCounts.ReplicationHubCount = ongoingTasks.PullReplications.Count;
        taskCounts.Total += ongoingTasks.PullReplications.Count;

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (TasksInfo.BackupTasks != null)
        {
            var lastBackup = TasksInfo.BackupTasks.Where(x => x.LastFullBackup != null || x.LastIncrementalBackup != null)
                .OrderBy(x => GetLaterDate(x.LastFullBackup, x.LastIncrementalBackup)).LastOrDefault();

            if (lastBackup != null)
            {
                var lastBackupDate = GetLaterDate(lastBackup.LastFullBackup, lastBackup.LastIncrementalBackup);

                double intervalUntilNextBackupInSec = -1;

                if (lastBackup.NextBackup != null)
                {
                    intervalUntilNextBackupInSec = (lastBackup.NextBackup.DateTime - lastBackupDate).TotalSeconds;
                }

                TasksInfo.LastBackupInfo = new BackupInfo
                {
                    LastBackup = lastBackupDate,
                    BackupTaskType = lastBackup?.TaskId == 0 ? BackupTaskType.OneTime : BackupTaskType.Periodic,
                    Destinations = lastBackup.BackupDestinations,
                    IntervalUntilNextBackupInSec = intervalUntilNextBackupInSec
                };

                if (_debugInfoCollectedAt != null)
                {
                    var ago = _debugInfoCollectedAt - lastBackupDate;

                    if (ago > TimeSpan.FromDays(7))
                    {
                        issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                            $"Last backup was taken {ago.Value.TotalDays} days ago",
                            $"Last backup was taken on {lastBackupDate:yyyy-MM-dd HH:mm:ss}." +
                            "This may indicate that the backup is not running or that the backup is not working properly.",
                            IssueSeverity.Warning,
                            IssueCategory.Database));
                    }
                }
            }

            DateTime GetLaterDate(DateTime? date1, DateTime? date2)
            {
                if (date1 == null) return date2!.Value;
                if (date2 == null) return date1!.Value;
                return date1 > date2 ? date1.Value : date2.Value;
            }
        }
    }
}
