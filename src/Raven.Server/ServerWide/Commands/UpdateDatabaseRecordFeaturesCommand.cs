using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands
{
    // Base for all UpdateDatabaseCommand types that represent features.
    // Used to handle license-limited features and allow skipping license
    // asserts when a feature is being disabled.
    public abstract class UpdateDatabaseRecordFeaturesCommand : UpdateDatabaseCommand
    {
        protected UpdateDatabaseRecordFeaturesCommand() { }

        protected UpdateDatabaseRecordFeaturesCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId) { }

        public virtual bool Disabled => false;

        public override void AssertBeforeAppend(ClusterOperationContext context, ServerStore serverStore, long index)
        {
            // Clone the command (from Raw) and the database record, and run the update on the clones: the license checks
            // see the record exactly as apply will produce it, in isolation, and this command and the stored record do not change.
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
