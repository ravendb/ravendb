using System;
using System.Linq;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationCommand : UpdateDatabaseCommand
    {
        public ExternalReplication Watcher;

        public UpdateExternalReplicationCommand()
        {

        }

        public UpdateExternalReplicationCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Watcher.AssertValidReplication();

            if (Watcher == null)
                return ;

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = etag;                
            }
            else
            {
                //modified watcher, remove the old one
                ExternalReplicationBase.RemoveExternalReplication(record.ExternalReplications, Watcher.TaskId);
            }
            //this covers the case of a new watcher and edit of an old watcher
            if (string.IsNullOrEmpty(Watcher.Name))
            {
                Watcher.Name = record.EnsureUniqueTaskName(Watcher.GetDefaultTaskName());
            }

            if (Watcher.Name.StartsWith(ServerWideExternalReplication.NamePrefix, StringComparison.OrdinalIgnoreCase))
            {
                var isNewTask = record.ExternalReplications.Exists(x => x.Name.Equals(Watcher.Name, StringComparison.OrdinalIgnoreCase));
                throw new InvalidOperationException($"Can't {(isNewTask ? "create" : "update")} task: '{Watcher.Name}'. " +
                                                    $"A regular (non server-wide) external replication name can't start with prefix '{ServerWideExternalReplication.NamePrefix}'");
            }

            EnsureTaskNameIsNotUsed(record, Watcher.Name);

            record.ExternalReplications.Add(Watcher);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54200, serverStore) == false)
                return;

            var licenseStatus = serverStore.LicenseManager.LoadAndGetLicenseStatus(serverStore);
            if (licenseStatus.HasDelayedExternalReplication)
                return;

            if (licenseStatus.HasExternalReplication == false && databaseRecord.ExternalReplications.Count > 0)
                throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding External Replication.");

            if (databaseRecord.ExternalReplications.All(exRep => exRep.DelayReplicationFor == TimeSpan.Zero))
                return;

            throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");

        }
    }
}
