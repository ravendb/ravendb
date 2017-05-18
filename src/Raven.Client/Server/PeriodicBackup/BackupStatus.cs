using System;
using System.Diagnostics;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.PeriodicBackup
{
    public abstract class BackupStatus
    {
        public DateTime? LastFullBackup { get; set; }

        public DateTime? LastIncrementalBackup { get; set; }

        public long? FullBackupDurationInMs { get; set; }

        public long? IncrementalBackupDurationInMs { get; set; }

        public Exception Exception { get; set; }

        public IDisposable Update(bool isFullBackup, Reference<Exception> exception)
        {
            var now = SystemTime.UtcNow;
            var sw = Stopwatch.StartNew();

            return new DisposableAction(() =>
            {
                if (isFullBackup)
                {
                    LastFullBackup = now;
                    FullBackupDurationInMs = sw.ElapsedMilliseconds;
                }
                else
                {
                    LastIncrementalBackup = now;
                    IncrementalBackupDurationInMs = sw.ElapsedMilliseconds;
                }

                Exception = exception.Value;
            });
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastFullBackup)] = LastFullBackup,
                [nameof(LastIncrementalBackup)] = LastIncrementalBackup,
                [nameof(FullBackupDurationInMs)] = FullBackupDurationInMs,
                [nameof(IncrementalBackupDurationInMs)] = IncrementalBackupDurationInMs,
                [nameof(Exception)] = Exception
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