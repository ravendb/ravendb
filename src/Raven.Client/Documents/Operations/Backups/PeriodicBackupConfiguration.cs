//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    /// <summary>
    /// Defines the configuration for periodic backup tasks.
    /// Supports full and incremental backups with configurable frequencies, retention policies, and mentor node assignments.
    /// </summary>
    public class PeriodicBackupConfiguration : BackupConfiguration, IDatabaseTask, IDynamicJsonValueConvertible
    {
        /// <summary>
        /// The name of the periodic backup task.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The unique identifier for the backup task.
        /// </summary>
        public long TaskId { get; set; }

        /// <summary>
        /// Indicates whether the backup task is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        /// <summary>
        /// The mentor node responsible for executing the backup task.
        /// </summary>
        public string MentorNode { get; set; }

        /// <summary>
        /// Determines if the backup task should always run on the mentor node.
        /// </summary>
        public bool PinToMentorNode { get; set; }

        /// <summary>
        /// The retention policy associated with the backup task.
        /// </summary>
        public RetentionPolicy RetentionPolicy { get; set; }

        /// <summary>
        /// The timestamp when the backup task was created.
        /// </summary>
        public DateTime? CreatedAt { get; set; }

        /// <summary>
        /// Frequency of full backup jobs in cron format
        /// </summary>
        public string FullBackupFrequency { get; set; }

        /// <summary>
        /// Frequency of incremental backup jobs in cron format
        /// If set to null incremental backup will be disabled.
        /// </summary>
        public string IncrementalBackupFrequency { get; set; }

        public ulong GetTaskKey()
        {
            Debug.Assert(TaskId != 0);

            return (ulong)TaskId;
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }

        public string GetDefaultTaskName()
        {
            var destinations = GetFullBackupDestinations();
            return destinations.Count == 0 ?
                $"{BackupType} w/o destinations" :
                $"{BackupType} to {string.Join(", ", destinations)}";
        }

        public string GetTaskName()
        {
            return Name;
        }

        public bool IsResourceIntensive()
        {
            return true;
        }

        public bool IsPinnedToMentorNode()
        {
            return PinToMentorNode;
        }

        public bool HasBackupFrequencyChanged(PeriodicBackupConfiguration other)
        {
            if (other == null)
                return true;

            if (Equals(other.FullBackupFrequency, FullBackupFrequency) == false)
                return true;

            if (Equals(other.IncrementalBackupFrequency, IncrementalBackupFrequency) == false)
                return true;

            return false;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Name)] = Name;
            json[nameof(TaskId)] = TaskId;
            json[nameof(Disabled)] = Disabled;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(PinToMentorNode)] = PinToMentorNode;
            json[nameof(FullBackupFrequency)] = FullBackupFrequency;
            json[nameof(IncrementalBackupFrequency)] = IncrementalBackupFrequency;
            json[nameof(RetentionPolicy)] = RetentionPolicy?.ToJson();
            json[nameof(CreatedAt)] = CreatedAt;
            return json;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            var json = base.ToAuditJson();
            json[nameof(Name)] = Name;
            json[nameof(TaskId)] = TaskId;
            json[nameof(Disabled)] = Disabled;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(PinToMentorNode)] = PinToMentorNode;
            json[nameof(FullBackupFrequency)] = FullBackupFrequency;
            json[nameof(IncrementalBackupFrequency)] = IncrementalBackupFrequency;
            json[nameof(RetentionPolicy)] = RetentionPolicy?.ToAuditJson();
            return json;
        }

        public override bool ValidateDestinations(out string message)
        {
            if (HasBackup() || Disabled)
            {
                message = null;
                return true;
            }

            message = "The backup configuration is enabled without target any destinations";
            return false;
        }
    }
}
