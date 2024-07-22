using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationAsSinkCommand : UpdateDatabaseCommand
    {
        public PullReplicationAsSink PullReplicationAsSink;
        public bool? UseServerCertificate;
        public UpdatePullReplicationAsSinkCommand()
        {

        }

        public UpdatePullReplicationAsSinkCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (PullReplicationAsSink == null)
                return ;

            if (PullReplicationAsSink.TaskId == 0)
            {
                PullReplicationAsSink.TaskId = etag;
            }
            else
            {
                // if new definition doesn't have certificate but there is old one with cert 
                // it means we want to use existing cert or the server certificate
                if (PullReplicationAsSink.CertificateWithPrivateKey == null)
                {
                    if (UseServerCertificate == true)
                    {
                        PullReplicationAsSink.CertificateWithPrivateKey = null;
                    }
                    else
                    {
                        var existingDefinition = record.SinkPullReplications.Find(x => x.TaskId == PullReplicationAsSink.TaskId);
                        if (existingDefinition?.CertificateWithPrivateKey != null)
                        {
                            // retain existing certificate
                            PullReplicationAsSink.CertificateWithPrivateKey = existingDefinition.CertificateWithPrivateKey;
                            PullReplicationAsSink.CertificatePassword = existingDefinition.CertificatePassword;
                        }
                    }
                }
                
                ExternalReplicationBase.RemoveExternalReplication(record.SinkPullReplications, PullReplicationAsSink.TaskId);
            }

            if (string.IsNullOrEmpty(PullReplicationAsSink.Name))
            {
                PullReplicationAsSink.Name = record.EnsureUniqueTaskName(PullReplicationAsSink.GetDefaultTaskName());
            }

            if (string.IsNullOrEmpty(PullReplicationAsSink.ConnectionStringName))
                throw new ArgumentNullException(nameof(PullReplicationAsSink.ConnectionStringName));

            record.EnsureTaskNameIsNotUsed(PullReplicationAsSink.Name);
            record.SinkPullReplications.Add(PullReplicationAsSink);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PullReplicationAsSink)] = PullReplicationAsSink.ToJson();
            json[nameof(UseServerCertificate)] = UseServerCertificate;
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasPullReplicationAsSink)
                return;

            if (databaseRecord.SinkPullReplications.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.PullReplicationAsSink, "Your license doesn't support adding Sink Replication feature.");

        }
    }
}
