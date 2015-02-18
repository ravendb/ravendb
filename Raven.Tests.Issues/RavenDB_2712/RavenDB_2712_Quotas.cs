// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
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
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

			    var globalSettings = new GlobalSettingsDocument
			    {
			        Settings =
			        {
			            {Constants.SizeSoftLimitInKB, "10"},
			            {Constants.SizeHardLimitInKB, "11"},
                        {Constants.DocsSoftLimit, "12"},
                        {Constants.DocsHardLimit, "13"}
			        }
			    };
			    systemDatabase.Documents.Put(Constants.Global.GlobalSettingsDocumentKey, null, RavenJObject.FromObject(globalSettings), new RavenJObject(), null);

				Assert.Equal("10", retriever.GetEffectiveConfigurationSetting(Constants.SizeSoftLimitInKB));
                Assert.Equal("11", retriever.GetEffectiveConfigurationSetting(Constants.SizeHardLimitInKB));
                Assert.Equal("12", retriever.GetEffectiveConfigurationSetting(Constants.DocsSoftLimit));
                Assert.Equal("13", retriever.GetEffectiveConfigurationSetting(Constants.DocsHardLimit));
			}
		}

		[Fact]
		public void GlobalDefaultConfigurationShouldNotBeEffectiveIfThereIsLocalDefault()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

                var globalSettings = new GlobalSettingsDocument
                {
                    Settings =
			        {
			            {Constants.SizeSoftLimitInKB, "10"},
			            {Constants.SizeHardLimitInKB, "11"},
                        {Constants.DocsSoftLimit, "12"},
                        {Constants.DocsHardLimit, "13"}
			        }
                };
                systemDatabase.Documents.Put(Constants.Global.GlobalSettingsDocumentKey, null, RavenJObject.FromObject(globalSettings), new RavenJObject(), null);

				database.Configuration.Settings[Constants.SizeSoftLimitInKB] = "20";
				database.Configuration.Settings[Constants.SizeHardLimitInKB] = "21";
				database.Configuration.Settings[Constants.DocsHardLimit] = "22";
				database.Configuration.Settings[Constants.DocsSoftLimit] = "23";

                Assert.Equal("20", retriever.GetEffectiveConfigurationSetting(Constants.SizeSoftLimitInKB));
                Assert.Equal("21", retriever.GetEffectiveConfigurationSetting(Constants.SizeHardLimitInKB));
                Assert.Equal("22", retriever.GetEffectiveConfigurationSetting(Constants.DocsHardLimit));
                Assert.Equal("23", retriever.GetEffectiveConfigurationSetting(Constants.DocsSoftLimit));
			}
		}
	}
}