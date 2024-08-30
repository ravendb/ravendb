using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_21412 : RavenTestBase
{
    public RavenDB_21412(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public void Validate_All_OngoingTaskTypes_Are_Added_To_Snmp()
    {
        var enumsAddedToSnmp = new HashSet<OngoingTaskType>
        {
            OngoingTaskType.Backup,
            OngoingTaskType.ElasticSearchEtl,
            OngoingTaskType.OlapEtl,
            OngoingTaskType.PullReplicationAsHub,
            OngoingTaskType.PullReplicationAsSink,
            OngoingTaskType.QueueEtl,
            OngoingTaskType.RavenEtl,
            OngoingTaskType.Replication,
            OngoingTaskType.SqlEtl,
            OngoingTaskType.Subscription
        };

        foreach (var enumValue in Enum.GetValues<OngoingTaskType>())
        {
            // if this test fails add counts in OngoingTasksBase and ActiveOngoingTasksBase
            // then take those counts and use them in TotalNumberOfOngoingTasks
            // add also 1x TotalNumberOfXXXTasks and 1x TotalNumberOfActiveXXXTasks
            // remember to register them in SnmpEngine
            Assert.True(enumsAddedToSnmp.Contains(enumValue), $"enumsAddedToSnmp.Contains({enumValue}) => please add '{enumValue}' as a part of 5.1.11 OID family and update the '{nameof(enumsAddedToSnmp)}' hashset.");
        }
    }

    [RavenFact(RavenTestCategory.Monitoring, LicenseRequired = true)]
    public async Task Verify_OngoingTasks_In_Snmp_Return_Proper_Counts()
    {
        DoNotReuseServer();

        using (var store = GetDocumentStore())
        {
            var total = new TotalNumberOfOngoingTasks(Server.ServerStore);
            var test = new TestOngoingTasks(Server.ServerStore, "1");

            AssertCounts(test, total, expectedOlapCount: 0, expectedElasticCount: 0, expectedExternalCount: 0, expectedBackupCount: 0, expectedQueueCount: 0, expectedRavenCount: 0, expectedSinkCount: 0, expectedSqlCount: 0, expectedSubscriptionCount: 0);

            var sub = await store.Subscriptions.CreateAsync<Company>();
            AssertCounts(test, total, expectedOlapCount: 0, expectedElasticCount: 0, expectedExternalCount: 0, expectedBackupCount: 0, expectedQueueCount: 0, expectedRavenCount: 0, expectedSinkCount: 0, expectedSqlCount: 0, expectedSubscriptionCount: 1);
            await store.Subscriptions.DisableAsync(sub);
            AssertCounts(test, total, expectedOlapCount: 0, expectedElasticCount: 0, expectedExternalCount: 0, expectedBackupCount: 0, expectedQueueCount: 0, expectedRavenCount: 0, expectedSinkCount: 0, expectedSqlCount: 0, expectedSubscriptionCount: 0);

            await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(new PeriodicBackupConfiguration { FullBackupFrequency = "* * * * *", LocalSettings = new LocalSettings { FolderPath = NewDataPath() } }));
            AssertCounts(test, total, expectedOlapCount: 0, expectedElasticCount: 0, expectedExternalCount: 0, expectedBackupCount: 1, expectedQueueCount: 0, expectedRavenCount: 0, expectedSinkCount: 0, expectedSqlCount: 0, expectedSubscriptionCount: 0);
        }
    }

    private static void AssertCounts(
        TestOngoingTasks test,
        TotalNumberOfOngoingTasks total,
        int expectedOlapCount,
        int expectedElasticCount,
        int expectedExternalCount,
        int expectedBackupCount,
        int expectedQueueCount,
        int expectedRavenCount,
        int expectedSinkCount,
        int expectedSqlCount,
        int expectedSubscriptionCount)
    {
        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfOlapEtls(database);
        Assert.Equal(expectedOlapCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfElasticSearchEtls(database);
        Assert.Equal(expectedElasticCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfExternalReplications(database);
        Assert.Equal(expectedExternalCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfPeriodicBackups(database);
        Assert.Equal(expectedBackupCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfQueueEtls(database);
        Assert.Equal(expectedQueueCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfRavenEtls(database);
        Assert.Equal(expectedRavenCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfSinkPullReplications(database);
        Assert.Equal(expectedSinkCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = (_, database) => OngoingTasksBase.GetNumberOfSqlEtls(database);
        Assert.Equal(expectedSqlCount, ((Integer32)test.Data).ToInt32());

        test.GetCountFunc = OngoingTasksBase.GetNumberOfSubscriptions;
        Assert.Equal(expectedSubscriptionCount, ((Integer32)test.Data).ToInt32());

        Assert.Equal(expectedOlapCount + expectedElasticCount + expectedExternalCount + expectedBackupCount + expectedQueueCount + expectedRavenCount + expectedSinkCount + expectedSqlCount + expectedSubscriptionCount, ((Integer32)total.Data).ToInt32());
    }

    private class TestOngoingTasks : OngoingTasksBase
    {
        public Func<TransactionOperationContext, RawDatabaseRecord, int> GetCountFunc;

        public TestOngoingTasks(ServerStore serverStore, string dots)
            : base(serverStore, dots)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            return GetCountFunc(context, database);
        }
    }
}
