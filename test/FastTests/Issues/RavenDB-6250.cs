// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4446.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Xunit;
using Xunit.Abstractions;

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

        [Fact]
        public void All_operations_has_details_providers()
        {
            var alreadyHandledInStudio = new HashSet<Operations.OperationType>
            {
                Operations.OperationType.UpdateByQuery,
                Operations.OperationType.DeleteByQuery,
                Operations.OperationType.DeleteByCollection,
                Operations.OperationType.DatabaseExport,
                Operations.OperationType.DatabaseImport,
                Operations.OperationType.DatabaseMigrationRavenDb,
                Operations.OperationType.DatabaseMigration,
                Operations.OperationType.DatabaseRestore,
                Operations.OperationType.BulkInsert,
                Operations.OperationType.IndexCompact,
                Operations.OperationType.DatabaseCompact,
                Operations.OperationType.CertificateGeneration,
                Operations.OperationType.MigrationFromLegacyData,
                Operations.OperationType.CollectionImportFromCsv,
                Operations.OperationType.DatabaseBackup,
                Operations.OperationType.MigrationFromSql,
                Operations.OperationType.RecordTransactionCommands,
                Operations.OperationType.ReplayTransactionCommands,
                Operations.OperationType.DatabaseRevert,
                Operations.OperationType.EnforceRevisionConfiguration
            };

            var operationWithoutDetails = new HashSet<Operations.OperationType>
            {
                Operations.OperationType.Setup, // it is in secured setup,
                Operations.OperationType.DebugPackage
            };

            var allKnownTypes = Enum.GetNames(typeof(Operations.OperationType)).ToHashSet();

            var unionSet = new HashSet<Operations.OperationType>(alreadyHandledInStudio);
            unionSet.UnionWith(operationWithoutDetails);

            var list = allKnownTypes.Except(unionSet.Select(x => x.ToString())).ToList();

            Assert.True(list.Count == 0, "Probably unhandled details for operations: " + string.Join(", ", list) +
                ". If those was already handled in notification center please add given type to 'alreadyHandledInStudio' set. " +
                                         "If operation doesn't provide details, please add this to 'operationWithoutDetails' set.");
        }

        [Fact]
        public void All_performance_hints_has_details_providers()
        {
            var alreadyHandledInStudio = new HashSet<PerformanceHintType>
            {
               PerformanceHintType.Paging,
               PerformanceHintType.Indexing,
               PerformanceHintType.RequestLatency,
               PerformanceHintType.UnusedCapacity,
               PerformanceHintType.SqlEtl_SlowSql,
               PerformanceHintType.HugeDocuments
            };

            var operationWithoutDetails = new HashSet<PerformanceHintType>
            {
                PerformanceHintType.None,
                PerformanceHintType.Replication,
                PerformanceHintType.SlowIO
            };

            var allKnownTypes = Enum.GetNames(typeof(PerformanceHintType)).ToHashSet();

            var unionSet = new HashSet<PerformanceHintType>(alreadyHandledInStudio);
            unionSet.UnionWith(operationWithoutDetails);

            var list = allKnownTypes.Except(unionSet.Select(x => x.ToString())).ToList();

            Assert.True(list.Count == 0, "Probably unhandled details for performance hints: " + string.Join(", ", list) +
                ". If those was already handled in notification center please add given type to 'alreadyHandledInStudio' set. " +
                                         "If operation doesn't provide details, please add this to 'operationWithoutDetails' set.");
        }


    }
}
