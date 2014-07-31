// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2432.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2432 : RavenTest
    {
        [Fact]
        public void ShouldChangeConcurrencyLevel()
        {
            using (var store = NewDocumentStore())
            {
               
                store.DatabaseCommands.Admin.StopIndexing();
                store.DatabaseCommands.Admin.StartIndexing(7);
                Assert.Equal(7, store.SystemDatabase.Configuration.MaxNumberOfParallelIndexTasks);

                store.DatabaseCommands.Admin.StopIndexing();
                store.AsyncDatabaseCommands.Admin.StartIndexingAsync(9).Wait();
                Assert.Equal(9, store.SystemDatabase.Configuration.MaxNumberOfParallelIndexTasks);

                store.DatabaseCommands.Admin.StopIndexing();
                store.DatabaseCommands.Admin.StartIndexing();
                Assert.Equal(9, store.SystemDatabase.Configuration.MaxNumberOfParallelIndexTasks);
            }
        }
    }
}