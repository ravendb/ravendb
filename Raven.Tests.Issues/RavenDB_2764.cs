// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2764.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2764 : RavenTest
	{
		private string ExportDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenDB_2764-Export");

		 public RavenDB_2764()
        {
			if (Directory.Exists(ExportDir))
				IOExtensions.DeleteDirectory(ExportDir);
        }

        public override void Dispose()
        {
            base.Dispose();

			if(Directory.Exists(ExportDir))
				IOExtensions.DeleteDirectory(ExportDir);
        }

		[Fact]
		public async Task ShouldSmuggleIdentitiesBetweenDatabases()
		{
			using (var server1 = GetNewServer(port: 8079))
            using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
			{
                using (var session = store1.OpenAsyncSession("Database1"))
                {
                    await session.StoreAsync(new User {Id = "users/", Name = "Arek"});
                    await session.SaveChangesAsync();
                }

				store1.DatabaseCommands.SeedIdentityFor("users/", 10);

                using (var server2 = GetNewServer(port: 8078))
                {
					using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
					{
						var smugglerApi = new SmugglerApi();
						await smugglerApi.Between(new SmugglerBetweenOptions
						{
							From = new RavenConnectionStringOptions {Url = "http://localhost:8079", DefaultDatabase = "Database1"},
							To = new RavenConnectionStringOptions {Url = "http://localhost:8078", DefaultDatabase = "Database2"}
						});
                    
                        using (var session = store2.OpenAsyncSession("Database2"))
                        {
							await session.StoreAsync(new User { Id = "users/", Name = "Oren" });
							await session.SaveChangesAsync();
                        }

						var documents = (await store2.AsyncDatabaseCommands.GetDocumentsAsync(0, 10)).OrderBy(x => x.Key).ToArray();

						Assert.Equal(2, documents.Length);
						Assert.Equal("users/1", documents[0].Key);
						Assert.Equal("users/11", documents[1].Key);
					}
                }
            }
		}

		[Fact]
		public async Task ShouldSmuggleIdentitiesInExportImport()
		{
			using (var server1 = GetNewServer(port: 8079))
			using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1, databaseName: "Database1"))
			{
				using (var session = store1.OpenAsyncSession("Database1"))
				{
					await session.StoreAsync(new User { Id = "users/", Name = "Arek" });
					await session.SaveChangesAsync();
				}

				store1.DatabaseCommands.SeedIdentityFor("users/", 10);

				var smugglerApi = new SmugglerApi();
				await smugglerApi.ExportData(new SmugglerExportOptions
				{
					From = new RavenConnectionStringOptions { Url = "http://localhost:8079", DefaultDatabase = "Database1" },
					ToFile = ExportDir
				});

				using (var server2 = GetNewServer(port: 8078))
				{
					using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2, databaseName: "Database2"))
					{
						await smugglerApi.ImportData(new SmugglerImportOptions
						{
							FromFile = ExportDir,
							To = new RavenConnectionStringOptions { Url = "http://localhost:8078", DefaultDatabase = "Database2" }
						});

						using (var session = store2.OpenAsyncSession("Database2"))
						{
							await session.StoreAsync(new User { Id = "users/", Name = "Oren" });
							await session.SaveChangesAsync();
						}

						var documents = (await store2.AsyncDatabaseCommands.GetDocumentsAsync(0, 10)).OrderBy(x => x.Key).ToArray();

						Assert.Equal(2, documents.Length);
						Assert.Equal("users/1", documents[0].Key);
						Assert.Equal("users/11", documents[1].Key);
					}
				}
			}
		}
	}
}