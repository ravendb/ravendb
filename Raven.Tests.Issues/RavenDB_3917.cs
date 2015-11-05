// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3917.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Remote;
using Raven.Smuggler.Database.Streams;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3917 : RavenTest
    {
        [Fact]
        public async Task SmugglerShouldNotExportImportSubscribtionIdentities()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.Subscriptions.Create(new SubscriptionCriteria());


                
                using (var stream = new MemoryStream())
                {
                    var options = new DatabaseSmugglerOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                    };

                    var smuggler = new DatabaseSmuggler(
                        options,
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url
                        }),
                        new DatabaseSmugglerStreamDestination(stream));

                    await smuggler.ExecuteAsync();

                    stream.Position = 0;

                    store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Northwind");

                    smuggler = new DatabaseSmuggler(
                        options,
                        new DatabaseSmugglerStreamSource(stream),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = "Northwind",
                            Url = store.Url
                        }));

                    await smuggler.ExecuteAsync();
                }
            }
        }
    }
}
