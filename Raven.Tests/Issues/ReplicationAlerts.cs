// -----------------------------------------------------------------------
//  <copyright file="ReplicationAlerts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
    public class ReplicationAlerts : ReplicationBase
    {
        public ReplicationAlerts()
        {
            if (File.Exists(DumpFile))
                File.Delete(DumpFile);
        }

        [Fact]
        public void ImportingReplicationDestinationsDocumentWithInvalidSourceShouldReportOneAlertOnly()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            store2.Dispose();

            store1.DatabaseCommands.Put("1", null, new RavenJObject(), new RavenJObject());
            store1.DatabaseCommands.Put("2", null, new RavenJObject(), new RavenJObject());

            var smuggler = new SmugglerApi(new RavenConnectionStringOptions
                                       {
                                           Url = store1.Url
                                       });

            smuggler.ExportData(new SmugglerExportOptions { ToFile = DumpFile }, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
            Assert.True(File.Exists(DumpFile));

            smuggler = new SmugglerApi(new RavenConnectionStringOptions
                                       {
                                           Url = store3.Url
                                       });
            smuggler.ImportData(new SmugglerImportOptions { FromFile = DumpFile }, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));

            Assert.NotNull(store3.DatabaseCommands.Get("1"));
            Assert.NotNull(store3.DatabaseCommands.Get("2"));

            var container = store3.DatabaseCommands.Get("Raven/Alerts");
            Assert.NotNull(container);

            var alerts = container.DataAsJson["Alerts"].Values<RavenJObject>()
                .ToList();
            Assert.Equal(1, alerts.Count);

            var alert = alerts.First();
            Assert.True(alert["Title"].ToString().StartsWith("Wrong replication source:"));
        }

        protected string DumpFile = "dump.ravendump";
    }
}