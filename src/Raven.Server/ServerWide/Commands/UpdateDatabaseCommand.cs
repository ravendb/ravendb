using System;
using Raven.Client.ServerWide;
using Raven.Server.Logging;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    // NOTE: For commands that represent license-limited features,
    //       inherit from UpdateDatabaseRecordFeaturesCommand instead.

    public abstract class UpdateDatabaseCommand : CommandBase
    {
        public string DatabaseName;
        public bool ErrorOnDatabaseDoesNotExists = true;

        protected UpdateDatabaseCommand() { }

        protected UpdateDatabaseCommand(string databaseName, string uniqueRequestId) : base(uniqueRequestId)
        {
            DatabaseName = databaseName;
        }

        public abstract void UpdateDatabaseRecord(DatabaseRecord record, long etag);

        public virtual void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, RavenAuditLogger clusterAuditLog)
        {

        }

        protected static void SetLeaderStampForTopology(DatabaseTopology topology, long etag)
        {
            topology.Stamp ??= new LeaderStamp { Term = -1, LeadersTicks = -1, Index = -1 };
            topology.Stamp.Index = etag;
        }

        public abstract void FillJson(DynamicJsonValue json);

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(DatabaseName)] = DatabaseName;
            djv[nameof(ErrorOnDatabaseDoesNotExists)] = ErrorOnDatabaseDoesNotExists;

            FillJson(djv);

            return djv;
        }

        public virtual void Initialize(ServerStore serverStore, ClusterOperationContext context)
        {
        }

        public static void EnsureTaskNameIsNotUsed(DatabaseRecord record, string name)
        {
            try
            {
                record.EnsureTaskNameIsNotUsed(name);
            }
            catch (Exception e)
            {
                throw new RachisApplyException($"Task name `{name}` is already in use", e);
            }
        }

        protected void AssertLicenseLimitsOnUpdatedRecord(ClusterOperationContext context, ServerStore serverStore, long index)
        {
            var databaseRecord = serverStore.Cluster.ReadDatabase(context, DatabaseName);
            if (databaseRecord == null)
                return;

            var type = GetType().Name;
            var command = (UpdateDatabaseCommand)JsonDeserializationCluster.Commands[type](Raw);

            try
            {
                command.Initialize(serverStore, context);
                command.UpdateDatabaseRecord(databaseRecord, index);
            }
            catch
            {
                return;
            }

            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);
            serverStore.Cluster.AssertLicenseLimits(type, serverStore, databaseRecord, items, context, command);
        }
    }
}
