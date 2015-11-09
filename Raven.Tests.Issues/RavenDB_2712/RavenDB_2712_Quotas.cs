// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
    public class RavenDB_2712_Quotas : GlobalConfigurationTest
    {
        [Fact]
        public void GlobalDefaultConfigurationShouldBeEffectiveIfThereIsNoLocal()
        {
            using (NewRemoteDocumentStore(databaseName: "Northwind"))
            {
                var server = servers[0];
                var systemDatabase = server.SystemDatabase;
                var database = AsyncHelpers.RunSync(() => server.Server.GetDatabaseInternal("Northwind"));
                var retriever = database.ConfigurationRetriever;

                var globalSettings = new GlobalSettingsDocument
                {
                    Settings =
                    {
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit), "10"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit), "11"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit), "12"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit), "13"}
                    }
                };
                systemDatabase.Documents.Put(Constants.Global.GlobalSettingsDocumentKey, null, RavenJObject.FromObject(globalSettings), new RavenJObject(), null);

                Assert.Equal("10", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit)));
                Assert.Equal("11", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit)));
                Assert.Equal("12", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit)));
                Assert.Equal("13", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit)));
            }
        }

        [Fact]
        public void GlobalDefaultConfigurationShouldNotBeEffectiveIfThereIsLocalDefault()
        {
            using (var server = GetNewServer())
            {
                server.DocumentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Northwind",
                    Settings =
                    {
                        {"Raven/DataDir", @"~\Databases\Mine"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit), "20"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit), "21"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit), "22"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit), "23"}
                    }
                });

                var systemDatabase = server.SystemDatabase;
                var database = AsyncHelpers.RunSync(() => server.Server.GetDatabaseInternal("Northwind"));
                var retriever = database.ConfigurationRetriever;

                var globalSettings = new GlobalSettingsDocument
                {
                    Settings =
                    {
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit), "10"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit), "11"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit), "12"},
                        {InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit), "13"}
                    }
                };
                systemDatabase.Documents.Put(Constants.Global.GlobalSettingsDocumentKey, null, RavenJObject.FromObject(globalSettings), new RavenJObject(), null);

                Assert.Equal("20", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeSoftLimit)));
                Assert.Equal("21", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.SizeHardLimit)));
                Assert.Equal("22", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsSoftLimit)));
                Assert.Equal("23", retriever.GetEffectiveConfigurationSetting(InMemoryRavenConfiguration.GetKey(x => x.Quotas.DocsHardLimit)));
            }
        }
    }
}
