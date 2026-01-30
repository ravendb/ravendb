using System.Text.RegularExpressions;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class NotificationSummaryItem
{
    public string Reason { get; set; }
    public string PrettifiedReason { get; set; }
    public long Count { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Reason)] = Reason,
            [nameof(PrettifiedReason)] = PrettifiedReason,
            [nameof(Count)] = Count
        };
    }
    
    public static string PrettifyReason(NotificationType notificationType, string reason)
    {
        return notificationType switch
        {
            NotificationType.PerformanceHint => reason switch
            {
                nameof(PerformanceHintReason.Paging) => "Requests: Page size too big",
                nameof(PerformanceHintReason.RequestLatency) => "Requests: Latency is too high",

                nameof(PerformanceHintReason.Indexing) => "Indexing: Definition issues",
                nameof(PerformanceHintReason.Indexing_References) => "Indexing: High load reference rate",

                nameof(PerformanceHintReason.SlowIO) => "Storage: An extremely slow write to disk",

                nameof(PerformanceHintReason.UnusedCapacity) => "System: Not all cores are used",

                nameof(PerformanceHintReason.Replication) => "Replication: Disabled destination",

                nameof(PerformanceHintReason.SqlEtl_SlowSql) => "ETL: Slow SQL detected",

                nameof(PerformanceHintReason.HugeDocuments) => "Huge documents impacting performance",

                _ => DefaultPrettify(reason)
            },

            NotificationType.AlertRaised => reason switch
            {
                nameof(AlertReason.QueueSink_Error) => "Queue Sink: Invalid configuration",
                nameof(AlertReason.QueueSink_ScriptError) => "Queue Sink: Could not parse script",
                nameof(AlertReason.QueueSink_ConsumeError) => "Queue Sink: Messages consumption has failed",
                nameof(AlertReason.QueueSink_ConsumerCreationError) => "Queue Sink: Failed to create consumer",

                nameof(AlertReason.OutOfMemoryException) => "System: Out of memory occurred",
                nameof(AlertReason.ServerLimits) => "System: Server is running close to OS limits",

                nameof(AlertReason.LowDiskSpace) => "Storage: Low free disk space",

                nameof(AlertReason.MismatchedReferenceLoad) => "Indexing: Loading documents with mismatched collection",

                nameof(AlertReason.Etl_LoadError) => "ETL: Loading transformed data to the destination has failed",

                nameof(AlertReason.BlockingTombstones) => "Blockage in tombstone deletion",
                nameof(AlertReason.PeriodicBackup) => "Failed to run backup",
                nameof(AlertReason.ConflictRevisionsExceeded) => "Excess number of Conflict Revisions",
                nameof(AlertReason.ReplicationMissingAttachments) => "Detected missing attachments for a document",

                _ => DefaultPrettify(reason)
            },
            
            _ => DefaultPrettify(reason)
        };
    }
    
    private static string DefaultPrettify(string reason)
    {
        var parts = reason.Split('_', 2);
        
        if (parts.Length < 2)
        {
            return AddSpacesToPascalCase(reason);
        }
        
        var firstPart = parts[0];
        var secondPart = parts[1];
        
        var formattedSecondPart = AddSpacesToPascalCase(secondPart);
        
        return $"{firstPart}: {formattedSecondPart}";
    }
    
    private static string AddSpacesToPascalCase(string text)
    {
        return Regex.Replace(
            text,
            "(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])",
            " ",
            RegexOptions.Compiled
        );
    }
}
