using System.Collections.Generic;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public abstract class OngoingTasksBase : DatabaseBase<Integer32>
{
    protected OngoingTasksBase(ServerStore serverStore, string dots)
        : base(serverStore, dots)
    {
    }

    protected override IEnumerable<RawDatabaseRecord> GetDatabases(TransactionOperationContext context)
    {
        foreach (var database in base.GetDatabases(context))
        {
            if (database.IsDisabled)
                continue;

            yield return database;
        }
    }

    protected static int GetNumberOfElasticSearchEtls(RawDatabaseRecord database) => database.ElasticSearchEtls?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfExternalReplications(RawDatabaseRecord database) => database.ExternalReplications?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfOlapEtls(RawDatabaseRecord database) => database.OlapEtls?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfPeriodicBackups(RawDatabaseRecord database) => database.PeriodicBackups?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfQueueEtls(RawDatabaseRecord database) => database.QueueEtls?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfRavenEtls(RawDatabaseRecord database) => database.RavenEtls?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfSinkPullReplications(RawDatabaseRecord database) => database.SinkPullReplications?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfSqlEtls(RawDatabaseRecord database) => database.SqlEtls?.Count(x => x.Disabled == false) ?? 0;

    protected static int GetNumberOfSubscriptions(TransactionOperationContext context, RawDatabaseRecord database)
    {
        var count = 0;
        foreach (var kvp in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(database.DatabaseName)))
        {
            if (kvp.Value.TryGet(nameof(SubscriptionState.Disabled), out bool disabled) && disabled)
                continue;

            count++;
        }

        return count;
    }
}
