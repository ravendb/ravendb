// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1735.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;

using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_1735 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void GetDatabaseSizeInBytesShouldWork(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: requestedStorage, runInMemory: false))
            {
                var statistics = store.DatabaseCommands.GlobalAdmin.GetStatistics();
                var databases = statistics.LoadedDatabases.ToList();

                Assert.Equal(2, databases.Count);
                Assert.True(databases[0].TotalDatabaseSize > 0);
                Assert.True(databases[1].TotalDatabaseSize > 0);

                Assert.True(databases[0].TransactionalStorageAllocatedSize > 0);
                Assert.True(databases[1].TransactionalStorageAllocatedSize > 0);

                Assert.True(databases[0].TransactionalStorageUsedSize > 0);
                Assert.True(databases[1].TransactionalStorageUsedSize > 0);

                Assert.True(databases[0].TransactionalStorageUsedSize <= databases[0].TransactionalStorageAllocatedSize);
                Assert.True(databases[1].TransactionalStorageUsedSize <= databases[1].TransactionalStorageAllocatedSize);
            }
        }
    }
}