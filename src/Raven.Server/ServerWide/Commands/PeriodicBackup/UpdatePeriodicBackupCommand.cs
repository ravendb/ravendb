using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupCommand : UpdateDatabaseCommand
    {
        public PeriodicBackupConfiguration Configuration;

        public UpdatePeriodicBackupCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            bool newTask = false;
            if (Configuration.TaskId == 0)
            {
                // this is a new backup configuration
                newTask = true;
                Configuration.TaskId = index;
            }
            else
            {
                // modified periodic backup, remove the old one
                record.DeletePeriodicBackupConfiguration(Configuration.TaskId);
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
            record.ClusterState.LastPeriodicBackupsIndex = index;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
