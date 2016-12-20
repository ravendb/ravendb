// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2808.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2808 : ReplicationBase
    {
        [Fact]
        public void DuringRestoreReplicationDestinationsCanBeDisabled()
        {
            var backupPath = NewDataPath();

            using (var store = NewRemoteDocumentStore(runInMemory: false))
            {
                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "N1", 
                        Settings =
                        {
                            { Constants.ActiveBundles, "Replication" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                var commands = store.DatabaseCommands.ForDatabase("N1");

                commands
                    .Put(
                        Constants.RavenReplicationDestinations,
                        null,
                        RavenJObject.FromObject(new ReplicationDocument
                        {
                            Destinations = new List<ReplicationDestination>
                            {
                                new ReplicationDestination { Database = "N2", Url = store.Url }
                            }
                        }),
                        new RavenJObject());

                commands.GlobalAdmin.StartBackup(backupPath, null, incremental: false, databaseName: "N1").WaitForCompletion();

                var operation = commands
                    .GlobalAdmin
                    .StartRestore(new DatabaseRestoreRequest
                    {
                        BackupLocation = backupPath,
                        DatabaseName = "N3",
                        DisableReplicationDestinations = true
                    });

                var status = operation.WaitForCompletion();

                var replicationDestinationsAsJson = commands
                    .ForDatabase("N3")
                    .Get(Constants.RavenReplicationDestinations);

                var replicationDocument = replicationDestinationsAsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
                Assert.Equal(1, replicationDocument.Destinations.Count);
                foreach (var destination in replicationDocument.Destinations)
                {
                    Assert.True(destination.Disabled);
                }
            }
        }
    }
}
