// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_Versioning.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_Versioning : GlobalConfigurationTest
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

				var document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.VersioningDefaultConfigurationDocumentName,
						null,
						RavenJObject.FromObject(new VersioningConfiguration
												{
													Exclude = true,
													ExcludeUnlessExplicit = true,
													Id = Constants.Global.VersioningDefaultConfigurationDocumentName,
													MaxRevisions = 17,
													PurgeOnDelete = true
												}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
                Assert.True(document.MergedDocument.Exclude);
                Assert.True(document.MergedDocument.ExcludeUnlessExplicit);
                Assert.Equal(Constants.Versioning.RavenVersioningDefaultConfiguration, document.MergedDocument.Id);
                Assert.Equal(17, document.MergedDocument.MaxRevisions);
                Assert.True(document.MergedDocument.PurgeOnDelete);
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

				var document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.VersioningDefaultConfigurationDocumentName,
						null,
						RavenJObject.FromObject(new VersioningConfiguration
						{
							Exclude = true,
							ExcludeUnlessExplicit = true,
							Id = Constants.Global.VersioningDefaultConfigurationDocumentName,
							MaxRevisions = 17,
							PurgeOnDelete = true
						}),
						new RavenJObject(),
						null);

				database
					.Documents
					.Put(
						Constants.Versioning.RavenVersioningDefaultConfiguration,
						null,
						RavenJObject.FromObject(new VersioningConfiguration
						{
							Exclude = false,
							ExcludeUnlessExplicit = false,
							Id = Constants.Versioning.RavenVersioningDefaultConfiguration,
							MaxRevisions = 5,
							PurgeOnDelete = false
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.False(document.MergedDocument.Exclude);
                Assert.False(document.MergedDocument.ExcludeUnlessExplicit);
                Assert.Equal(Constants.Versioning.RavenVersioningDefaultConfiguration, document.MergedDocument.Id);
                Assert.Equal(5, document.MergedDocument.MaxRevisions);
                Assert.False(document.MergedDocument.PurgeOnDelete);
			}
		}

		[Fact]
		public void GlobalSpecificConfigurationShouldBeEffectiveIfThereIsNoLocalSpecific()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				var document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.VersioningDocumentPrefix + "Orders",
						null,
						RavenJObject.FromObject(new VersioningConfiguration
						{
							Exclude = true,
							ExcludeUnlessExplicit = true,
							Id = Constants.Global.VersioningDefaultConfigurationDocumentName,
							MaxRevisions = 17,
							PurgeOnDelete = true
						}),
						new RavenJObject(),
						null);

				database
					.Documents
					.Put(
						Constants.Versioning.RavenVersioningDefaultConfiguration,
						null,
						RavenJObject.FromObject(new VersioningConfiguration
						{
							Exclude = false,
							ExcludeUnlessExplicit = false,
							Id = Constants.Global.VersioningDefaultConfigurationDocumentName,
							MaxRevisions = 5,
							PurgeOnDelete = false
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningPrefix + "Orders");

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
                Assert.True(document.MergedDocument.Exclude);
                Assert.True(document.MergedDocument.ExcludeUnlessExplicit);
                Assert.Equal(Constants.Versioning.RavenVersioningDefaultConfiguration, document.MergedDocument.Id);
                Assert.Equal(17, document.MergedDocument.MaxRevisions);
                Assert.True(document.MergedDocument.PurgeOnDelete);
			}
		}

		[Fact]
		public void GlobalSpecificConfigurationShouldNotBeEffectiveIfThereIsLocalSpecific()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				var document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.VersioningDocumentPrefix + "Orders",
						null,
						RavenJObject.FromObject(new VersioningConfiguration
						{
							Exclude = true,
							ExcludeUnlessExplicit = true,
							Id = Constants.Global.VersioningDefaultConfigurationDocumentName,
							MaxRevisions = 17,
							PurgeOnDelete = true
						}),
						new RavenJObject(),
						null);

				database
					.Documents
					.Put(
						Constants.Versioning.RavenVersioningPrefix + "Orders",
						null,
						RavenJObject.FromObject(new VersioningConfiguration
						{
							Exclude = false,
							ExcludeUnlessExplicit = false,
							Id = Constants.Versioning.RavenVersioningDefaultConfiguration,
							MaxRevisions = 5,
							PurgeOnDelete = false
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningPrefix + "Orders");

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.False(document.MergedDocument.Exclude);
                Assert.False(document.MergedDocument.ExcludeUnlessExplicit);
                Assert.Equal(Constants.Versioning.RavenVersioningDefaultConfiguration, document.MergedDocument.Id);
                Assert.Equal(5, document.MergedDocument.MaxRevisions);
                Assert.False(document.MergedDocument.PurgeOnDelete);
			}
		}
	}
}