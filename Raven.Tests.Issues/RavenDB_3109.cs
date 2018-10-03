// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3109.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
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

        [Fact(Skip = "Flaky test")]
        public void ShouldWork()
        {
            using (var store1 = CreateStore(requestedStorageType: "esent"))
            using (var store2 = CreateStore(requestedStorageType: "esent"))
            using (var store3 = CreateStore(requestedStorageType: "esent"))
            {
                DeployNorthwind(store1);

                TellFirstInstanceToReplicateToSecondInstance();

                Assert.True(SpinWait.SpinUntil(() => store2.DatabaseCommands.Get("shippers/1") == null, Debugger.IsAttached ? TimeSpan.FromSeconds(30) : TimeSpan.FromSeconds(15)));

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

                 WaitForReplication(store3, session =>
                 {
                     var alerts = session.Load<AlertsDocument>(Constants.RavenAlerts);
                     if (alerts == null)
                         return false;

                     return (alerts.Alerts.Any(alert => alert.Title.Contains("Replication error. Multiple databases replicating at the same time with same DatabaseId")));
                 }, "DBX");
                
                SystemTime.UtcDateTime = () => DateTime.Now.AddMinutes(11);

                using (var session = store3.OpenSession("DBX"))
                {
                    var shipper = session.Load<Shipper>("shippers/1");

                    shipper.Name = "test";

                    session.SaveChanges();
                }

                WaitForReplication(store2, session =>
                {
                    var shipper = session.Load<Shipper>("shippers/1");
                    if (shipper == null)
                        return false;

                    return shipper.Name == "test";
                });
            }
        }
    }
}

