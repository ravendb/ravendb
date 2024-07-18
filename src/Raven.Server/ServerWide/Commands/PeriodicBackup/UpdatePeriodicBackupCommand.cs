using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Commercial;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupCommand : UpdateDatabaseCommand
    {
        public PeriodicBackupConfiguration Configuration;
        private bool _shouldRemoveBackupStatus;

        public UpdatePeriodicBackupCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName, string uniqueRequestId) 
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            bool newTask = false;
            if (Configuration.TaskId == 0)
            {
                // this is a new backup configuration
                newTask = true;
                Configuration.TaskId = etag;
                Configuration.CreatedAt = DateTime.UtcNow;
            }
            else
            {
                // modified periodic backup, remove the old one
                var previousBackupConfiguration = record.DeletePeriodicBackupConfiguration(Configuration.TaskId);
                if (previousBackupConfiguration != null && BackupHelper.BackupTypeChanged(previousBackupConfiguration, Configuration))
                    _shouldRemoveBackupStatus = true;
                
            }
            
            if (string.IsNullOrEmpty(Configuration.Name))
            {
                Configuration.Name = record.EnsureUniqueTaskName(Configuration.GetDefaultTaskName());
            }
            else if (Configuration.Name.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Can't {(newTask ? "create" : "update")} task: '{Configuration.Name}'. " +
                                                             $"A regular (non server-wide) backup task name can't start with prefix '{ServerWideBackupConfiguration.NamePrefix}'");
            }

            EnsureTaskNameIsNotUsed(record, Configuration.Name);

            record.PeriodicBackups.Add(Configuration);
        }

        public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, Logger clusterAuditLog)
        {
            if (_shouldRemoveBackupStatus == false)
                return;

            var taskName = PeriodicBackupStatus.GenerateItemName(DatabaseName, Configuration.TaskId);

            using (Slice.From(ctx.Allocator, taskName, out Slice _))
            using (Slice.From(ctx.Allocator, taskName.ToLowerInvariant(), out Slice keyNameLowered))
            {
                items.DeleteByKey(keyNameLowered);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54200, serverStore) == false)
                return;

            if (Configuration != null)
            {
                if (Configuration.BackupType == BackupType.Backup &&
                    Configuration.HasCloudBackup() == false &&
                    Configuration.BackupEncryptionSettings.Key == null)
                    return;
            }

            var backupTypes = LicenseManager.GetBackupTypes(databaseRecord.PeriodicBackups);
            var licenseStatus = serverStore.LicenseManager.LoadAndGetLicenseStatus(serverStore);
            if (backupTypes.HasSnapshotBackup)
                if (licenseStatus.HasSnapshotBackups == false)
                    throw new LicenseLimitException(LimitType.SnapshotBackup, "Your license doesn't support adding Snapshot backups feature.");

            if (backupTypes.HasCloudBackup)
                if (licenseStatus.HasCloudBackups == false)
                    throw new LicenseLimitException(LimitType.CloudBackup, "Your license doesn't support adding Cloud backups feature.");

            if (backupTypes.HasEncryptedBackup)
                if (licenseStatus.HasEncryptedBackups == false)
                    throw new LicenseLimitException(LimitType.EncryptedBackup, "Your license doesn't support adding Encrypted backups feature.");
        }
    }
}
