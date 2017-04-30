// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4446.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Extensions;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Xunit;

namespace SlowTests.Issues
{

    /**
     * Those convention tests guards against exception in Studio, when opening notification details. 
     * */
    public class RavenDB_6250 : NoDisposalNeeded
    {
        [Fact]
        public void All_operations_has_details_providers()
        {
            var alreadyHandledInStudio = new HashSet<DatabaseOperations.OperationType>
            {
                DatabaseOperations.OperationType.UpdateByIndex,
                DatabaseOperations.OperationType.DeleteByIndex,
                DatabaseOperations.OperationType.UpdateByCollection,
                DatabaseOperations.OperationType.DeleteByCollection,
                DatabaseOperations.OperationType.DatabaseExport,
                DatabaseOperations.OperationType.DatabaseImport,
                DatabaseOperations.OperationType.BulkInsert,
                DatabaseOperations.OperationType.IndexCompact
            };

            var operationWithoutDetails = new HashSet<DatabaseOperations.OperationType>
            {
                // empty for now
            };

            var allKnownTypes = Enum.GetNames(typeof(DatabaseOperations.OperationType)).ToHashSet();

            var unionSet = new HashSet<DatabaseOperations.OperationType>(alreadyHandledInStudio);
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
            };

            var operationWithoutDetails = new HashSet<PerformanceHintType>
            {
                PerformanceHintType.None,
                PerformanceHintType.Replication
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