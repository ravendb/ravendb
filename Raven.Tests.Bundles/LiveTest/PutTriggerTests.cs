// -----------------------------------------------------------------------
//  <copyright file="PutTriggerTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using System.Configuration;

using Raven.Abstractions.Data;
using Raven.Bundles.LiveTest;
using Raven.Client.Connection;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.LiveTest
{
    public class PutTriggerTests : RavenTest
    {
        protected override void ModifyConfiguration(RavenConfiguration configuration)
        {
            configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(LiveTestDatabaseDocumentPutTrigger).Assembly));
        }

        [Fact]
        public void PutTriggerShouldEnableQuotas()
        {
            using (var store = NewDocumentStore())
            {
                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "Northwind",
                        Settings =
                        {
                            { "Raven/ActiveBundles", "Replication" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                var document = store.DatabaseCommands.Get("Raven/Databases/Northwind");
                Assert.NotNull(document);

                var databaseDocument = document.DataAsJson.Deserialize<DatabaseDocument>(store.Conventions);
                AsserDatabaseDocument(databaseDocument);

                databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit)] = "123";
                databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit)] = "321";
                databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit)] = "456";
                databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit)] = "654";
                databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";

                store.DatabaseCommands.Put("Raven/Databases/Northwind", null, RavenJObject.FromObject(databaseDocument), document.Metadata);

                document = store.DatabaseCommands.Get("Raven/Databases/Northwind");
                Assert.NotNull(document);

                databaseDocument = document.DataAsJson.Deserialize<DatabaseDocument>(store.Conventions);
                AsserDatabaseDocument(databaseDocument);
            }
        }

        private static void AsserDatabaseDocument(DatabaseDocument databaseDocument)
        {
            var activeBundles = databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Core._ActiveBundlesString)].GetSemicolonSeparatedValues();

            Assert.Contains("Replication", activeBundles);

            Assert.Contains("Quotas", activeBundles);
            Assert.Equal(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Quotas/Size/HardLimitInKB"], databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit)]);
            Assert.Equal(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Quotas/Size/SoftLimitInKB"], databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit)]);
            Assert.Null(databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit)]);
            Assert.Null(databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit)]);

            Assert.True(bool.Parse(databaseDocument.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)]));
        }
    }
}
