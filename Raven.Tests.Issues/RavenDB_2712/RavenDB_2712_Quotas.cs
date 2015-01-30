// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_Quotas : RavenTest
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

				systemDatabase.Configuration.Settings[Constants.Global.QuotasSizeSoftLimitInKBSettingKey] = "10";
				systemDatabase.Configuration.Settings[Constants.Global.QuotasSizeHardLimitInKBSettingKey] = "11";
				systemDatabase.Configuration.Settings[Constants.Global.QuotasDocsHardLimitSettingKey] = "12";
				systemDatabase.Configuration.Settings[Constants.Global.QuotasDocsSoftLimitSettingKey] = "13";

				Assert.Equal("10", retriever.GetConfigurationSetting(Constants.SizeSoftLimitInKB));
				Assert.Equal("11", retriever.GetConfigurationSetting(Constants.SizeHardLimitInKB));
				Assert.Equal("12", retriever.GetConfigurationSetting(Constants.DocsHardLimit));
				Assert.Equal("13", retriever.GetConfigurationSetting(Constants.DocsSoftLimit));
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

				systemDatabase.Configuration.Settings[Constants.Global.QuotasSizeSoftLimitInKBSettingKey] = "10";
				systemDatabase.Configuration.Settings[Constants.Global.QuotasSizeHardLimitInKBSettingKey] = "11";
				systemDatabase.Configuration.Settings[Constants.Global.QuotasDocsHardLimitSettingKey] = "12";
				systemDatabase.Configuration.Settings[Constants.Global.QuotasDocsSoftLimitSettingKey] = "13";

				database.Configuration.Settings[Constants.SizeSoftLimitInKB] = "20";
				database.Configuration.Settings[Constants.SizeHardLimitInKB] = "21";
				database.Configuration.Settings[Constants.DocsHardLimit] = "22";
				database.Configuration.Settings[Constants.DocsSoftLimit] = "23";

				Assert.Equal("20", retriever.GetConfigurationSetting(Constants.SizeSoftLimitInKB));
				Assert.Equal("21", retriever.GetConfigurationSetting(Constants.SizeHardLimitInKB));
				Assert.Equal("22", retriever.GetConfigurationSetting(Constants.DocsHardLimit));
				Assert.Equal("23", retriever.GetConfigurationSetting(Constants.DocsSoftLimit));
			}
		}
	}
}