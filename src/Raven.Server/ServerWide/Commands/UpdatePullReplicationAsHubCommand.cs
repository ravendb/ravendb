using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationAsHubCommand : UpdateDatabaseCommand
    {
        public PullReplicationDefinition Definition;

        public UpdatePullReplicationAsHubCommand() { }

        public UpdatePullReplicationAsHubCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Definition.TaskId == 0)
            {
                Definition.TaskId = etag;
            }
            else
            {
                foreach (var task in record.HubPullReplications)
                {
                    if (task.TaskId != Definition.TaskId)
                        continue;
                    record.HubPullReplications.Remove(task);
                    break;
                }
            }
            
            record.EnsureTaskNameIsNotUsed(Definition.Name);
            record.HubPullReplications.Add(Definition);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasPullReplicationAsHub)
                return;

            if (databaseRecord.HubPullReplications.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.PullReplicationAsHub, "Your license doesn't support adding Hub Replication feature.");

        }
    }
}
