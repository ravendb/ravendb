// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveExternalReplicationTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActiveExternalReplicationTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveExternalReplicationTasks)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var rachisState = ServerStore.CurrentRachisState;
                var nodeTag = ServerStore.NodeTag;

                foreach (var database in GetDatabases(context))
                {
                    count += GetNumberOfActiveExternalReplications(rachisState, nodeTag, database);
                }
            }

            return new Integer32(count);
        }
    }
}
