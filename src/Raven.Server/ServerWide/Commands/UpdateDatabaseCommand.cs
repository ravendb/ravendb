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
    }
}
