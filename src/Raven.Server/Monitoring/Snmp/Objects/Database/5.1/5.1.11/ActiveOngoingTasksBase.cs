using System.Collections.Generic;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public abstract class ActiveOngoingTasksBase : DatabaseBase<Integer32>
{
    protected ActiveOngoingTasksBase(ServerStore serverStore, string dots)
        : base(serverStore, dots)
    {
    }

    protected override IEnumerable<RawDatabaseRecord> GetDatabases(TransactionOperationContext context)
    {
        foreach (var database in base.GetDatabases(context))
        {
            if (database.IsDisabled)
                continue;

            if (ServerStore.DatabasesLandlord.IsDatabaseLoaded(database.DatabaseName) == false)
                continue;

            yield return database;
        }
    }

    protected static int GetNumberOfActiveElasticSearchEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.ElasticSearchEtls.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveExternalReplications(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.ExternalReplications.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveOlapEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.OlapEtls.Where(x => x.Disabled == false));

    protected static int GetNumberOfActivePeriodicBackups(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.PeriodicBackups.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveQueueEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.QueueEtls.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveRavenEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.RavenEtls.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveSinkPullReplications(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.SinkPullReplications.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveSqlEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.SqlEtls.Where(x => x.Disabled == false));

    protected static int GetNumberOfActiveSubscriptions(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        var count = 0;
        foreach (var kvp in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(database.DatabaseName)))
        {
            var subscriptionState = JsonDeserializationClient.SubscriptionState(kvp.Value);
            if (subscriptionState.Disabled)
                continue;

            var responsibleNode = database.Topology.WhoseTaskIsIt(rachisState, subscriptionState);
            if (responsibleNode != nodeTag)
                continue;

            count++;
        }

        return count;
    }

    private static int CountTasks(RachisState rachisState, string nodeTag, DatabaseTopology databaseTopology, IEnumerable<IDatabaseTask> tasks)
    {
        if (tasks == null)
            return 0;

        var count = 0;
        foreach (var task in tasks)
        {
            var responsibleNode = databaseTopology.WhoseTaskIsIt(rachisState, task);
            if (responsibleNode != nodeTag)
                continue;

            count++;
        }

        return count;
    }
}
