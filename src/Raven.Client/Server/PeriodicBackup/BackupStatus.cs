using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.PeriodicBackup
{
    public abstract class BackupStatus
    {
        public DateTime LastFullBackup { get; set; }

        public DateTime LastIncrementalBackup { get; set; }

        public long DurationInMs { get; set; }

        public void Update(bool isFullBackup, long durationInMs)
        {
            if (isFullBackup)
                LastFullBackup = SystemTime.UtcNow;
            else
                LastIncrementalBackup = SystemTime.UtcNow;

            DurationInMs = durationInMs;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastFullBackup)] = LastFullBackup,
                [nameof(LastIncrementalBackup)] = LastIncrementalBackup,
                [nameof(DurationInMs)] = DurationInMs
            };
        }
    }

    public class LocalBackupStatus : BackupStatus
    {
        public string BackupDirectory { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[BackupDirectory] = BackupDirectory;
            return json;
        }
    }

    public class S3BackupStatus : BackupStatus
    {
        
    }

    public class GlacierBackupStatus : BackupStatus
    {

    }

    public class AzureBackupStatus : BackupStatus
    {

    }
}