using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Http;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class RavenCommandTest : RavenTestBase
    {
        public RavenCommandTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WhenCommandCanBeCheckedForFastestNode_ItCanRunInParallel()
        {
            var expected = new[]
            {
                "GetClusterTopologyCommand", "GetDatabaseTopologyCommand", "GetNodeInfoCommand", "GetRawStreamResultCommand", "GetTcpInfoCommand",
                "IsDatabaseLoadedCommand", "CreateSubscriptionCommand", "ExplainQueryCommand", "GetConflictsCommand", "GetDocumentsCommand",
                "GetNextOperationIdCommand", "GetRemoteTaskTopologyCommand", "GetRevisionsBinEntryCommand", "GetRevisionsCommand", "GetSubscriptionsCommand",
                "GetSubscriptionStateCommand", "GetTcpInfoForRemoteTaskCommand", "HeadAttachmentCommand", "HeadDocumentCommand", "NextHiLoCommand",
                "NextIdentityForCommand", "PutDocumentCommand", "QueryCommand", "QueryStreamCommand", "SeedIdentityForCommand", "StreamCommand", "MultiGetCommand",
                "SingleNodeBatchCommand", "AddDatabaseNodeCommand", "CompactDatabaseCommand", "ConfigureRevisionsForConflictsCommand", "CreateDatabaseCommand",
                "DeleteDatabaseCommand", "GetBuildNumberCommand", "GetDatabaseNamesCommand", "GetDatabaseRecordCommand", "GetServerWideOperationStateCommand",
                "ModifyConflictSolverCommand", "PromoteDatabaseNodeCommand", "RestoreBackupCommand", "ToggleDatabaseStateCommand", "OfflineMigrationCommand",
                "GetLogsConfigurationCommand", "GetServerWideBackupConfigurationCommand", "GetServerWideBackupConfigurationsCommand",
                "GetServerWideClientConfigurationCommand", "PutServerWideClientConfigurationCommand", "CreateClientCertificateCommand", "GetCertificateCommand",
                "GetCertificatesCommand", "DeleteByQueryCommand", "GetCollectionStatisticsCommand", "GetDetailedCollectionStatisticsCommand",
                "DetailedDatabaseStatisticsCommand", "GetOperationStateCommand", "GetStatisticsCommand", "PatchByQueryCommand", "PatchCommand",
                "ReplayTransactionsRecordingCommand", "ConfigureRevisionsCommand", "GetReplicationPerformanceStatisticsCommand",
                "UpdatePullReplicationDefinitionCommand", "UpdateExternalReplication", "UpdatePullEdgeReplication", "ConfigureRefreshCommand",
                "DeleteOngoingTaskCommand", "GetOngoingTaskInfoCommand", "GetPullReplicationTasksInfoCommand", "ToggleTaskStateCommand", "GetIndexErrorsCommand",
                "GetIndexesCommand", "GetIndexesStatisticsCommand", "GetIndexingStatusCommand", "GetIndexNamesCommand", "GetIndexCommand",
                "GetIndexPerformanceStatisticsCommand", "GetIndexStatisticsCommand", "GetTermsCommand", "IndexHasChangedCommand", "PutIndexesCommand",
                "GetIdentitiesCommand", "ConfigureExpirationCommand", "AddEtlCommand", "UpdateEtlCommand", "CounterBatchCommand", "GetCounterValuesCommand",
                "GetConnectionStringCommand", "PutConnectionStringCommand", "RemoveConnectionStringCommand", "GetClientConfigurationCommand",
                "DeleteCompareExchangeValueCommand", "GetCompareExchangeValueCommand", "GetCompareExchangeValuesCommand", "PutCompareExchangeValueCommand",
                "GetPeriodicBackupStatusCommand", "StartBackupCommand", "UpdatePeriodicBackupCommand", "GetAttachmentCommand", "PutAttachmentCommand",
                "BulkInsertCommand", "ClusterWideBatchCommand", "BatchCommand", "GetCertificatesMetadataCommand", "GetCertificateMetadataCommand",
                "AddClusterNodeCommand", "CloseTcpConnectionCommand", "DatabaseHealthCheckCommand", "DeleteAttachmentCommand", "DeleteCertificateCommand",
                "DeleteDocumentCommand", "DeleteIndexCommand", "DeleteServerWideBackupConfigurationCommand", "DeleteSorterCommand", "DeleteSubscriptionCommand",
                "DemoteClusterNodeCommand", "DisableIndexCommand", "DisableIndexCommand", "DropSubscriptionConnectionCommand", "EditClientCertificateCommand",
                "EnableIndexCommand", "ExportCommand", "HiLoReturnCommand", "ImportCommand", "KillOperationCommand", "PromoteClusterNodeCommand",
                "PutClientCertificateCommand", "PutClientConfigurationCommand", "PutSecretKeyCommand", "PutSortersCommand", "RemoveClusterNodeCommand",
                "ReorderDatabaseMembersCommand", "ReplaceClusterCertificateCommand", "ResetEtlCommand", "ResetIndexCommand", "SetDatabaseDynamicDistributionCommand",
                "SetIndexLockCommand", "SetIndexPriorityCommand", "SetLogsConfigurationCommand", "StartIndexCommand", "StartIndexingCommand",
                "StartTransactionsRecordingCommand", "StopIndexCommand", "StopIndexingCommand", "StopTransactionsRecordingCommand",
                "UpdateUnusedDatabasesCommand", "WaitForRaftIndexCommand",
                "BulkInsertCommand", "ClusterWideBatchCommand", "BatchCommand", "ConfigureTimeSeriesCommand", "ConfigureTimeSeriesPolicyCommand", "ConfigureTimeSeriesValue",
                "UpdateSubscriptionCommand", "RemoveTimeSeriesPolicyCommand", "GetTimeSeriesStatisticsCommand", "GetTimeSeriesCommand", "GetMultipleTimeSeriesCommand",
                "GetAttachmentsCommand", "ConfigureTimeSeriesValueNamesCommand",
                "DeleteIndexErrorsCommand", "TimeSeriesBatchCommand"
            }.OrderBy(t => t);

            var commandBaseType = typeof(RavenCommand<>);
            var types = commandBaseType.Assembly.GetTypes();

            var results = new List<Type>();
            GetAllDerivedTypesRecursively(types.Where(t => t.IsAbstract == false).ToArray(), commandBaseType, results);

            var actual = results.Select(r => r.Name).OrderBy(t => t);
            var didntCheck = actual.Except(expected).ToArray();
            Assert.False(didntCheck.Any(),
                $"The follow `{nameof(RavenCommand)}`s was not added to checked list: {string.Join(", ", didntCheck.Select(n => $"'{n}'"))}{Environment.NewLine}" +
                $"You should check if the commands can run in `{nameof(ReadBalanceBehavior.FastestNode)}` mode (checked in RequestExecutor.ShouldExecuteOnAll) " +
                $"and if it can so it can also run in parallel{Environment.NewLine}" +
                $"For more information look at - https://issues.hibernatingrhinos.com/issue/RavenDB-14286");
        }

        private static void GetAllDerivedTypesRecursively(Type[] types, Type type, List<Type> results)
        {
            if (type.IsGenericType)
            {
                GetDerivedFromGeneric(types, type, results);
            }
            else
            {
                GetDerivedFromNonGeneric(types, type, results);
            }
        }

        private static void GetDerivedFromGeneric(Type[] types, Type type, List<Type> results)
        {
            var derivedTypes = types
                .Where(t => IsDrivenFromGenericType(type, t)).ToList();
            results.AddRange(derivedTypes);
            foreach (var derivedType in derivedTypes)
            {
                GetAllDerivedTypesRecursively(types, derivedType, results);
            }
        }

        private static bool IsDrivenFromGenericType(Type baseType, Type type)
        {
            while (true)
            {
                if (type.BaseType == null || type.BaseType == typeof(object))
                    return false;

                if (type.BaseType.IsGenericType && type.BaseType.GetGenericTypeDefinition() == baseType)
                    return true;
                type = type.BaseType;
            }
        }

        private static void GetDerivedFromNonGeneric(Type[] types, Type type, List<Type> results)
        {
            var derivedTypes = types.Where(t => t != type && type.IsAssignableFrom(t)).ToList();

            results.AddRange(derivedTypes);
            foreach (var derivedType in derivedTypes)
            {
                GetAllDerivedTypesRecursively(types, derivedType, results);
            }
        }
    }
}
