// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3109.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3109 : ReplicationBase
    {
        private class Shipper
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Phone { get; set; }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store1 = CreateStore(requestedStorageType: "esent"))
            using (var store2 = CreateStore(requestedStorageType: "esent"))
            using (var store3 = CreateStore(requestedStorageType: "esent"))
            {
                DeployNorthwind(store1);

                TellFirstInstanceToReplicateToSecondInstance();

                WaitForDocument(store2.DatabaseCommands, "shippers/1");

                var path = NewDataPath();

                store1
                    .DatabaseCommands
                    .GlobalAdmin
                    .StartBackup(path, null, false, store1.DefaultDatabase)
                    .WaitForCompletion();

                store3
                    .DatabaseCommands
                    .GlobalAdmin
                    .StartRestore(new DatabaseRestoreRequest
                                  {
                                      BackupLocation = path,
                                      DatabaseName = "DBX"
                                  })
                    .WaitForCompletion();

                WaitForDocument(store3.DatabaseCommands.ForDatabase("DBX"), Constants.RavenAlerts);

                using (var session = store3.OpenSession("DBX"))
                {
                    var alerts = session.Load<AlertsDocument>(Constants.RavenAlerts);

                    Assert.True(alerts.Alerts.Any(alert=> alert.Title.Contains("Replication error. Multiple databases replicating at the same time with same DatabaseId")));
                }

                SystemTime.UtcDateTime = () => DateTime.Now.AddMinutes(11);

                using (var session = store3.OpenSession("DBX"))
                {
                    var shipper = session.Load<Shipper>("shippers/1");

                    shipper.Name = "test";

                    session.SaveChanges();
                }

                WaitForReplication(store2, session => session.Load<Shipper>("shippers/1").Name == "test");
            }
        }
    }
}
