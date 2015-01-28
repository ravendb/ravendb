// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_Versioning.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2712_Versioning : RavenTest
	{
		[Fact]
		public void IfThereIsNoLocalConfigurationThenGlobalShouldBeUsed()
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
						Constants.Versioning.RavenVersioningDefaultConfiguration,
						null,
						RavenJObject.FromObject(new VersioningConfiguration
												{
													Exclude = true,
													ExcludeUnlessExplicit = true,
													Id = Constants.Global.RavenGlobalVersioningDefaultConfiguration,
													MaxRevisions = 17,
													PurgeOnDelete = true
												}),
						new RavenJObject(),
						null);

				document = retriever.GetConfigurationDocument<VersioningConfiguration>(Constants.Versioning.RavenVersioningDefaultConfiguration);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
				Assert.True(document.Document.Exclude);
				Assert.True(document.Document.ExcludeUnlessExplicit);
				Assert.Equal(Constants.Versioning.RavenVersioningDefaultConfiguration, document.Document.Id);
				Assert.Equal(17, document.Document.MaxRevisions);
				Assert.True(document.Document.PurgeOnDelete);
			}
		}
	}
}