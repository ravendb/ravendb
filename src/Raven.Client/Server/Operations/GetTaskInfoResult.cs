using System;
using System.Collections.Generic;
using Raven.Client.Server.PeriodicBackup;

namespace Raven.Client.Server.Operations
{
    public enum OngoingTaskType
    {
        Replication,
        RavenEtl,
        SqlEtl,
        Backup,
        Subscription
    }

    public enum OngoingTaskState
    {
        Enabled,
        Disabled,
        PartiallyEnabled
    }

    public enum OngoingTaskConnectionStatus
    {
        Active,
        NotActive
    }

    public class GetTaskInfoResult
    {
        public long TaskId { get; set; }
        public string Name { get; set; }
        public OngoingTaskType TaskType { get; set; }
        public NodeId ResponsibleNode { get; set; }
        public OngoingTaskState TaskState { get; set; }
        public DateTime LastModificationTime { get; set; }
        public OngoingTaskConnectionStatus TaskConnectionStatus { get; set; }
        public string DestinationUrl { get; set; }
        public string DestinationDatabase { get; set; }
        public string DestinationServer { get; set; }   
        public string SqlProvider { get; set; }
        public string SqlTable { get; set; }
        public BackupType? BackupType { get; set; }
        public List<string> BackupDestinations { get; set; }
    }

    public class ModifyOngoingTaskResult
    {
        public long TaskId { get; set; }
    }
}