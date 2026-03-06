using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{

    /**
     * Those convention tests guards against exception in Studio, when opening notification details. 
     * */
    public class RavenDB_6250 : NoDisposalNeeded
    {
        public RavenDB_6250(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public void All_operations_has_details_providers()
        {
            var alreadyHandledInStudio = new HashSet<OperationType>
            {
                OperationType.UpdateByQuery,
                OperationType.DeleteByQuery,
                OperationType.DeleteByCollection,
                OperationType.DatabaseExport,
                OperationType.DatabaseImport,
                OperationType.DatabaseMigrationRavenDb,
                OperationType.DatabaseMigration,
                OperationType.DatabaseRestore,
                OperationType.BulkInsert,
                OperationType.IndexCompact,
                OperationType.DatabaseCompact,
                OperationType.CertificateGeneration,
                OperationType.MigrationFromLegacyData,
                OperationType.CollectionImportFromCsv,
                OperationType.DatabaseBackup,
                OperationType.MigrationFromSql,
                OperationType.RecordTransactionCommands,
                OperationType.ReplayTransactionCommands,
                OperationType.DatabaseRevert,
                OperationType.EnforceRevisionConfiguration,
                OperationType.DumpRawIndexData,
                OperationType.Resharding,
                OperationType.AdoptOrphanedRevisions,
                OperationType.ValidateSchema
            };

            var operationWithoutDetails = new HashSet<OperationType>
            {
                OperationType.Setup, // it is in secured setup,
                OperationType.DebugPackage,
                OperationType.LuceneOptimizeIndex
            };

            var allKnownTypes = Enum.GetNames(typeof(OperationType)).ToHashSet();

            var unionSet = new HashSet<OperationType>(alreadyHandledInStudio);
            unionSet.UnionWith(operationWithoutDetails);

            var list = allKnownTypes.Except(unionSet.Select(x => x.ToString())).ToList();

            Assert.True(list.Count == 0, "Probably unhandled details for operations: " + string.Join(", ", list) +
                ". If those was already handled in notification center please add given type to 'alreadyHandledInStudio' set. " +
                                         "If operation doesn't provide details, please add this to 'operationWithoutDetails' set.");
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public void All_performance_hints_has_details_providers()
        {
            var alreadyHandledInStudio = new HashSet<PerformanceHintReason>
            {
               PerformanceHintReason.Paging,
               PerformanceHintReason.Indexing,
               PerformanceHintReason.RequestLatency,
               PerformanceHintReason.UnusedCapacity,
               PerformanceHintReason.SqlEtl_SlowSql,
               PerformanceHintReason.HugeDocuments,
               PerformanceHintReason.Indexing_References
            };

            var operationWithoutDetails = new HashSet<PerformanceHintReason>
            {
                PerformanceHintReason.None,
                PerformanceHintReason.Replication,
                PerformanceHintReason.SlowIO
            };

            var allKnownTypes = Enum.GetNames(typeof(PerformanceHintReason)).ToHashSet();

            var unionSet = new HashSet<PerformanceHintReason>(alreadyHandledInStudio);
            unionSet.UnionWith(operationWithoutDetails);

            var list = allKnownTypes.Except(unionSet.Select(x => x.ToString())).ToList();

            Assert.True(list.Count == 0, "Probably unhandled details for performance hints: " + string.Join(", ", list) +
                ". If those was already handled in notification center please add given type to 'alreadyHandledInStudio' set. " +
                                         "If operation doesn't provide details, please add this to 'operationWithoutDetails' set.");
        }


    }
}
