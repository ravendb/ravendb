// -----------------------------------------------------------------------
//  <copyright file="SmugglerWithVersioning.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Client.Bundles.Versioning;
using Raven.Database.Smuggler.Embedded;
using Raven.Json.Linq;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Remote;
using Raven.Smuggler.Database.Streams;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Smuggler
{
	public class SmugglerWithDisabledVersioning_RavenDB_3219 : RavenTest
	{
		[Fact]
		public async Task CanDisableVersioningDuringImport_Remote()
		{
			using (var stream = new MemoryStream())
			{
				long countOfDocuments;
				using (var store = NewRemoteDocumentStore())
				{
					using (var session = store.OpenSession())
					{
						for (int i = 0; i < 10; i++)
						{
							session.Store(new User());
							session.Store(new Address());
						}

						session.SaveChanges();
					}

					countOfDocuments = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }),
                        new DatabaseSmugglerStreamDestination(stream));

				    await smuggler.ExecuteAsync();
				}

				stream.Position = 0;

				using (var store = NewRemoteDocumentStore(activeBundles: "Versioning"))
				{
					using (var session = store.OpenSession())
					{
						session.Store(new Bundles.Versioning.Data.VersioningConfiguration
						{
							Exclude = false,
							Id = "Raven/Versioning/DefaultConfiguration",
							MaxRevisions = 5
						});

						session.SaveChanges();
					}

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions
                        {
                            ShouldDisableVersioningBundle = true
                        },
                        new DatabaseSmugglerStreamSource(stream), 
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }));

                    await smuggler.ExecuteAsync();

					var countOfDocsAfterImport = store.DatabaseCommands.GetStatistics().CountOfDocuments;

					Assert.Equal(countOfDocuments, countOfDocsAfterImport - 1); // one additional doc for versioning bundle configuration

					var metadata = store.DatabaseCommands.Get("users/1").Metadata;

					Assert.True(metadata.ContainsKey(Constants.RavenIgnoreVersioning) == false, "Metadata contains temporary " + Constants.RavenIgnoreVersioning + " marker");

					// after import versioning should be active
					using (var session = store.OpenSession())
					{
						session.Store(new User(), "users/arek");

						session.SaveChanges();

						var revisionsFor = session.Advanced.GetRevisionsFor<User>("users/arek", 0, 10);

						Assert.Equal(1, revisionsFor.Length);
					}
				}
			}
		}

		[Fact]
		public async Task CanDisableVersioningDuringImport_Embedded()
		{
			using (var stream = new MemoryStream())
			{
				long countOfDocuments;
				using (var store = NewDocumentStore())
				{
					using (var session = store.OpenSession())
					{
						for (int i = 0; i < 10; i++)
						{
							session.Store(new User());
							session.Store(new Address());
						}

						session.SaveChanges();
					}

					countOfDocuments = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerEmbeddedSource(store.DocumentDatabase),
                        new DatabaseSmugglerStreamDestination(stream));

				    await smuggler.ExecuteAsync();
				}

				stream.Position = 0;

				using (var store = NewDocumentStore(activeBundles: "Versioning"))
				{
					using (var session = store.OpenSession())
					{
						session.Store(new Bundles.Versioning.Data.VersioningConfiguration
						{
							Exclude = false,
							Id = "Raven/Versioning/DefaultConfiguration",
							MaxRevisions = 5
						});

						session.SaveChanges();
					}

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions { ShouldDisableVersioningBundle = true },
                        new DatabaseSmugglerStreamSource(stream), 
                        new DatabaseSmugglerEmbeddedDestination(store.DocumentDatabase));

                    await smuggler.ExecuteAsync();

					var countOfDocsAfterImport = store.DatabaseCommands.GetStatistics().CountOfDocuments;

					Assert.Equal(countOfDocuments, countOfDocsAfterImport - 1); // one additional doc for versioning bundle configuration

					var metadata = store.DatabaseCommands.Get("users/1").Metadata;

					Assert.True(metadata.ContainsKey(Constants.RavenIgnoreVersioning) == false, "Metadata contains temporary " + Constants.RavenIgnoreVersioning + " marker");

					// after import versioning should be active
					using (var session = store.OpenSession())
					{
						session.Store(new User(), "users/arek");

						session.SaveChanges();

						var revisionsFor = session.Advanced.GetRevisionsFor<User>("users/arek", 0, 10);

						Assert.Equal(1, revisionsFor.Length);
					}
				}
			}
		}

		[Fact]
		public async Task CanDisableVersioningDuringImport_Between_Remote()
		{
			using (var server = GetNewServer())
			{
				using (var store = NewRemoteDocumentStore(ravenDbServer: server))
				{
					store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Import",
						Settings =
						{
							{ Constants.ActiveBundles, "Versioning" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

					using (var session = store.OpenSession())
					{
						for (int i = 0; i < 10; i++)
						{
							session.Store(new User());
							session.Store(new Address());
						}

						session.SaveChanges();
					}

					var countOfDocuments = store.DatabaseCommands.GetStatistics().CountOfDocuments;

					store.DatabaseCommands.ForDatabase("Import").Put("Raven/Versioning/DefaultConfiguration", null, RavenJObject.FromObject(new Bundles.Versioning.Data.VersioningConfiguration
					{
						Exclude = false,
						Id = "Raven/Versioning/DefaultConfiguration",
						MaxRevisions = 5
					}), new RavenJObject());


                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions { ShouldDisableVersioningBundle = true },
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions { Url = store.Url, Database = store.DefaultDatabase }),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions { Url = store.Url, Database = "Import" }));

                    await smuggler.ExecuteAsync();

					var countOfDocsAfterImport = store.DatabaseCommands.ForDatabase("Import").GetStatistics().CountOfDocuments;

					Assert.Equal(countOfDocuments, countOfDocsAfterImport - 1); // one additional doc for versioning bundle configuration

					var metadata = store.DatabaseCommands.ForDatabase("Import").Get("users/1").Metadata;

					Assert.True(metadata.ContainsKey(Constants.RavenIgnoreVersioning) == false, "Metadata contains temporary " + Constants.RavenIgnoreVersioning + " marker");

					// after import versioning should be active
					using (var session = store.OpenSession("Import"))
					{
						session.Store(new User(), "users/arek");

						session.SaveChanges();

						var revisionsFor = session.Advanced.GetRevisionsFor<User>("users/arek", 0, 10);

						Assert.Equal(1, revisionsFor.Length);
					}
				}

			}
		}
	}
}