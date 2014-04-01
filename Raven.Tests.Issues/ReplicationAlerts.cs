// -----------------------------------------------------------------------
//  <copyright file="ReplicationAlerts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class ReplicationAlerts : ReplicationBase
    {
		protected string DumpFile = "dump.ravendump";

        public ReplicationAlerts()
        {
            if (File.Exists(DumpFile))
                File.Delete(DumpFile);
        }

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.DefaultStorageTypeName = GetDefaultStorageType("esent");
            configuration.RunInMemory = false;
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

	        var smuggler = new SmugglerApi();

			smuggler.ExportData(new SmugglerExportOptions
			{
				ToFile = DumpFile,
				From = new RavenConnectionStringOptions
				{
					Url = store1.Url,
					DefaultDatabase = store1.DefaultDatabase
				}
			}, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
            Assert.True(File.Exists(DumpFile));

	        smuggler = new SmugglerApi();
			smuggler.ImportData(new SmugglerImportOptions
			{
				FromFile = DumpFile,
				To = new RavenConnectionStringOptions
				{
					Url = store3.Url,
					DefaultDatabase = store3.DefaultDatabase
				}
			}, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));

            Assert.NotNull(store3.DatabaseCommands.Get("1"));
            Assert.NotNull(store3.DatabaseCommands.Get("2"));

	        int retries = 5;
	        JsonDocument container = null;
			while (container == null && retries-- >0)
	        {
		        container = store3.DatabaseCommands.Get("Raven/Alerts");
				if(container == null)
					Thread.Sleep(100);
	        }
	        Assert.NotNull(container);

            var alerts = container.DataAsJson["Alerts"].Values<RavenJObject>()
                .ToList();
            Assert.Equal(1, alerts.Count);

            var alert = alerts.First();
            Assert.True(alert["Title"].ToString().StartsWith("Wrong replication source:"));
        }
    }
}