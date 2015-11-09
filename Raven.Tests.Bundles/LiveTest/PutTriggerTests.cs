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
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
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

                databaseDocument.Settings[Constants.SizeHardLimitInKB] = "123";
                databaseDocument.Settings[Constants.SizeSoftLimitInKB] = "321";
                databaseDocument.Settings[Constants.DocsHardLimit] = "456";
                databaseDocument.Settings[Constants.DocsSoftLimit] = "654";
                databaseDocument.Settings[InMemoryRavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";

                store.DatabaseCommands.Put("Raven/Databases/Northwind", null, RavenJObject.FromObject(databaseDocument), document.Metadata);

                document = store.DatabaseCommands.Get("Raven/Databases/Northwind");
                Assert.NotNull(document);

                databaseDocument = document.DataAsJson.Deserialize<DatabaseDocument>(store.Conventions);
                AsserDatabaseDocument(databaseDocument);
            }
        }

        private static void AsserDatabaseDocument(DatabaseDocument databaseDocument)
        {
            var activeBundles = databaseDocument.Settings[InMemoryRavenConfiguration.GetKey(x => x.Core.ActiveBundlesStringValue)].GetSemicolonSeparatedValues();

            Assert.Contains("Replication", activeBundles);

            Assert.Contains("Quotas", activeBundles);
            Assert.Equal(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Quotas/Size/HardLimitInKB"], databaseDocument.Settings[Constants.SizeHardLimitInKB]);
            Assert.Equal(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Quotas/Size/SoftLimitInKB"], databaseDocument.Settings[Constants.SizeSoftLimitInKB]);
            Assert.Null(databaseDocument.Settings[Constants.DocsHardLimit]);
            Assert.Null(databaseDocument.Settings[Constants.DocsSoftLimit]);

            Assert.True(bool.Parse(databaseDocument.Settings[InMemoryRavenConfiguration.GetKey(x => x.Core.RunInMemory)]));
        }
    }
}
