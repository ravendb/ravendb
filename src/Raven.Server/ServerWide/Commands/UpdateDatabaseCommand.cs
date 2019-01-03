using System;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateDatabaseCommand : CommandBase
    {
        public string DatabaseName;
        public bool ErrorOnDatabaseDoesNotExists;

        protected UpdateDatabaseCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public abstract string UpdateDatabaseRecord(DatabaseRecord record, long etag);

        public abstract void FillJson(DynamicJsonValue json);

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(DatabaseName)] = DatabaseName;

            FillJson(djv);

            return djv;
        }

        public virtual void Initialize(ServerStore serverStore, TransactionOperationContext context)
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
