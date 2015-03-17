// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2391.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Counters;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.FileSystem;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2391 : RavenTest
	{
		[Fact]
		public void SecurityForDatabasesCountersAndFileSystemsShouldBeConfiguredInOnePlaceAndWorkByName()
		{
			Authentication.EnableOnce();
			using (var server = GetNewServer(enableAuthentication: true, runInMemory: false))
			{
				server
					.SystemDatabase
					.Documents
					.Put(
						"Raven/Databases/DB1",
						null,
						RavenJObject.FromObject(
							new DatabaseDocument
							{
								Id = "DB1",
								Settings =
								{
									{"Raven/ActiveBundles", "PeriodicBackup"},
									{"Raven/DataDir", "~\\Databases\\DB1"}
								}
							}),
						new RavenJObject(),
						null);

				server
					.SystemDatabase
					.Documents
					.Put(
						"Raven/ApiKeys/key1",
						null,
						RavenJObject.FromObject(
							new ApiKeyDefinition
							{
								Name = "key1", 
								Secret = "ThisIsMySecret", 
								Enabled = true, 
								Databases = new List<ResourceAccess>
								            {
									            new ResourceAccess { TenantId = Constants.SystemDatabase, Admin = true },
												new ResourceAccess { TenantId = "DB1", Admin = true }
								            }
							}),
						new RavenJObject(),
						null);

				server
					.SystemDatabase
					.Documents
					.Put(
						"Raven/ApiKeys/key2",
						null,
						RavenJObject.FromObject(
							new ApiKeyDefinition
							{
								Name = "key2",
								Secret = "ThisIsMySecret2",
								Enabled = true,
								Databases = new List<ResourceAccess>
								            {
									            new ResourceAccess { TenantId = Constants.SystemDatabase, Admin = true },
												new ResourceAccess { TenantId = "DB2", Admin = true }
								            }
							}),
						new RavenJObject(),
						null);

				using (var store = new FilesStore
				                   {
					                   Url = server.Options.SystemDatabase.ServerUrl,
									   DefaultFileSystem = "DB1",
									   ApiKey = "key1/ThisIsMySecret"
				                   }.Initialize(ensureFileSystemExists: true))
				{
					var files = store.AsyncFilesCommands.BrowseAsync().ResultUnwrap();
				}

				using (var store = new DocumentStore
				                   {
					                   Url = server.Options.SystemDatabase.ServerUrl,
									   DefaultDatabase = "DB1",
									   ApiKey = "key1/ThisIsMySecret"
				                   }.Initialize())
				{
					var indexes = store.DatabaseCommands.GetIndexNames(0, 10);
				}

				using (var store = new FilesStore
				{
					Url = server.Options.SystemDatabase.ServerUrl,
					DefaultFileSystem = "DB1",
					ApiKey = "key2/ThisIsMySecret2"
				}.Initialize())
				{
					var exception = Assert.Throws<ErrorResponseException>(() => store.AsyncFilesCommands.BrowseAsync().ResultUnwrap());
					Assert.Equal(HttpStatusCode.Forbidden, exception.StatusCode);
				}

				using (var store = new DocumentStore
				{
					Url = server.Options.SystemDatabase.ServerUrl,
					DefaultDatabase = "DB1",
					ApiKey = "key2/ThisIsMySecret2"
				}.Initialize())
				{
					var exception = Assert.Throws<ErrorResponseException>(() => store.DatabaseCommands.GetIndexNames(0, 10));
					Assert.Equal(HttpStatusCode.Forbidden, exception.StatusCode);
				}
			}
			
		}
	}
}