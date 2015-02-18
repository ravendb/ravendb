// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_PeriodicExport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_PeriodicExport : GlobalConfigurationTest
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

				var document = retriever.GetConfigurationDocument<PeriodicExportSetup>(PeriodicExportSetup.RavenDocumentKey);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.PeriodicExportDocumentName,
						null,
						RavenJObject.FromObject(new PeriodicExportSetup
						{
							AwsRegionEndpoint = "e1",
							AzureStorageContainer = "c1",
							Disabled = true,
							FullBackupIntervalMilliseconds = 17,
							GlacierVaultName = "g1",
							IntervalMilliseconds = 12,
							LocalFolderName = "f1",
							S3BucketName = "s1"
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<PeriodicExportSetup>(PeriodicExportSetup.RavenDocumentKey);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
				Assert.Equal("e1", document.MergedDocument.AwsRegionEndpoint);
                Assert.Equal("c1", document.MergedDocument.AzureStorageContainer);
                Assert.True(document.MergedDocument.Disabled);
                Assert.Equal(17, document.MergedDocument.FullBackupIntervalMilliseconds);
                Assert.Equal("g1", document.MergedDocument.GlacierVaultName);
                Assert.Equal(12, document.MergedDocument.IntervalMilliseconds);
                Assert.Equal("f1\\Northwind", document.MergedDocument.LocalFolderName);
                Assert.Equal("s1", document.MergedDocument.S3BucketName);
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

				var document = retriever.GetConfigurationDocument<PeriodicExportSetup>(PeriodicExportSetup.RavenDocumentKey);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.PeriodicExportDocumentName,
						null,
						RavenJObject.FromObject(new PeriodicExportSetup
						{
							AwsRegionEndpoint = "e1",
							AzureStorageContainer = "c1",
							Disabled = true,
							FullBackupIntervalMilliseconds = 17,
							GlacierVaultName = "g1",
							IntervalMilliseconds = 12,
							LocalFolderName = "f1",
							S3BucketName = "s1"
						}),
						new RavenJObject(),
						null);

				database
					.Documents
					.Put(
						PeriodicExportSetup.RavenDocumentKey,
						null,
						RavenJObject.FromObject(new PeriodicExportSetup
						{
							AwsRegionEndpoint = "e2",
							AzureStorageContainer = "c2",
							Disabled = false,
							FullBackupIntervalMilliseconds = 2,
							GlacierVaultName = "g2",
							IntervalMilliseconds = 16,
							LocalFolderName = "f2",
							S3BucketName = "s2"
						}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<PeriodicExportSetup>(PeriodicExportSetup.RavenDocumentKey);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.Equal("e2", document.MergedDocument.AwsRegionEndpoint);
                Assert.Equal("c2", document.MergedDocument.AzureStorageContainer);
                Assert.False(document.MergedDocument.Disabled);
                Assert.Equal(2, document.MergedDocument.FullBackupIntervalMilliseconds);
                Assert.Equal("g2", document.MergedDocument.GlacierVaultName);
                Assert.Equal(16, document.MergedDocument.IntervalMilliseconds);
                Assert.Equal("f2", document.MergedDocument.LocalFolderName);
                Assert.Equal("s2", document.MergedDocument.S3BucketName);
			}
		}
	}
}