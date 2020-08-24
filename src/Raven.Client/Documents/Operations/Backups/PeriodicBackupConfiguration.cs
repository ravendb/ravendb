//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class PeriodicBackupConfiguration : BackupConfiguration, IDatabaseTask, IDynamicJsonValueConvertible
    {
        public string Name { get; set; }
        public long TaskId { get; set; }
        public bool Disabled { get; set; }
        public string MentorNode { get; set; }
        public RetentionPolicy RetentionPolicy { get; set; }

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
            json[nameof(FullBackupFrequency)] = FullBackupFrequency;
            json[nameof(IncrementalBackupFrequency)] = IncrementalBackupFrequency;
            json[nameof(RetentionPolicy)] = RetentionPolicy?.ToJson();
            return json;
        }
    }
}
