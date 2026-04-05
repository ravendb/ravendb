using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class UpdatePullReplicationAsSinkCommand : UpdateDatabaseRecordFeaturesCommand
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

            PullReplicationPathFilterUtils.NormalizeAndValidate(ref PullReplicationAsSink.AllowedHubToSinkPaths, ref PullReplicationAsSink.AllowedSinkToHubPaths, PullReplicationAsSink.Name ?? PullReplicationAsSink.HubName);

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

        public override bool Disabled => PullReplicationAsSink.Disabled;
    }
}
