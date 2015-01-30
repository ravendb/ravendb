// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712 : RavenTest
	{
		[Fact]
		public void DocumentRetrieverShouldThrowIfSettingIsNotSupported()
		{
			using (var store = NewDocumentStore())
			{
				var retriever = store.DocumentDatabase.ConfigurationRetriever;

				Assert.Throws<NotSupportedException>(() => retriever.GetConfigurationSetting("notSupportedKey"));
			}
		}

		[Fact]
		public void DocumentRetrieverSubscribtionsShouldThrowIfDocumentTypeIsNotSupported()
		{
			using (var store = NewDocumentStore())
			{
				var retriever = store.DocumentDatabase.ConfigurationRetriever;

				Assert.Throws<NotSupportedException>(() => retriever.SubscribeToConfigurationDocumentChanges("notSupportedKey", () => { }));
			}
		}

		[Fact]
		public void DocumentRetrieverSubscribtionsShouldWork()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var manualResetEventSlim = new ManualResetEventSlim();

				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				retriever.SubscribeToConfigurationDocumentChanges(PeriodicExportSetup.RavenDocumentKey, manualResetEventSlim.Set);

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

				if (manualResetEventSlim.Wait(TimeSpan.FromSeconds(10)) == false)
					throw new InvalidOperationException("Waited for 10 seconds for notification.");

				manualResetEventSlim.Reset();

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

				if (manualResetEventSlim.Wait(TimeSpan.FromSeconds(10)) == false)
					throw new InvalidOperationException("Waited for 10 seconds for notification.");
			}
		}
	}
}