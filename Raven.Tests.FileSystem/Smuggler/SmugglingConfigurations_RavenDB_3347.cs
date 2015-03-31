// -----------------------------------------------------------------------
//  <copyright file="SmugglingConfigurations_RavenDB_3347.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Smuggler
{
	public class SmugglingConfigurations_RavenDB_3347 : RavenFilesTestBase
	{
		[Fact, Trait("Category", "Smuggler")]
		public async Task ShouldExportAndImportConfigurations()
		{
			using (var exportStream = new MemoryStream())
			{
				int countOfConfigurations;

				using (var store = NewStore())
				{
					for (int i = 0; i < 100; i++)
					{
						await store.AsyncFilesCommands.Configuration.SetKeyAsync("items/" + i, new RavenJObject
						{
							{
								"test", "value"
							},
							{
								"test-array", new RavenJArray
								{
									"item-1", "item-2", "item-3"
								}
							}
						});
					}

					countOfConfigurations = (await store.AsyncFilesCommands.Configuration.GetKeyNamesAsync(0, 200)).Length;

					var exportOptions = new SmugglerExportOptions<FilesConnectionStringOptions>
					{
						From = new FilesConnectionStringOptions
						{
							Url = store.Url,
							DefaultFileSystem = store.DefaultFileSystem
						},
						ToStream = exportStream
					};

					await new SmugglerFilesApi().ExportData(exportOptions);
				}

				using (var import = NewStore(1))
				{
					exportStream.Position = 0;

					var importOptions = new SmugglerImportOptions<FilesConnectionStringOptions>
					{
						FromStream = exportStream,
						To = new FilesConnectionStringOptions()
						{
							Url = import.Url,
							DefaultFileSystem = import.DefaultFileSystem
						}
					};

					await new SmugglerFilesApi().ImportData(importOptions);

					Assert.Equal(countOfConfigurations, (await import.AsyncFilesCommands.Configuration.GetKeyNamesAsync(0, 200)).Length);

					for (int i = 0; i < 100; i++)
					{
						Assert.NotNull(await import.AsyncFilesCommands.Configuration.GetKeyAsync<RavenJObject>("items/" + i));
					}
				}
			}
		}

		[Fact, Trait("Category", "Smuggler")]
		public async Task ShouldSmuggleConfigurationsInBetweenOperation()
		{
			using (var exportStore = NewStore())
			using (var importStore = NewStore(1))
			{
				for (int i = 0; i < 100; i++)
				{
					await exportStore.AsyncFilesCommands.Configuration.SetKeyAsync("items/" + i, new RavenJObject
						{
							{
								"test", "value"
							},
							{
								"test-array", new RavenJArray
								{
									"item-1", "item-2", "item-3"
								}
							}
						});
				}

				var countOfConfigurations = (await exportStore.AsyncFilesCommands.Configuration.GetKeyNamesAsync(0, 200)).Length;

				var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>()
				{
					From = new FilesConnectionStringOptions()
					{
						Url = exportStore.Url,
						DefaultFileSystem = exportStore.DefaultFileSystem
					},
					To = new FilesConnectionStringOptions()
					{
						Url = importStore.Url,
						DefaultFileSystem = importStore.DefaultFileSystem
					}
				};

				var smugglerFilesApi = new SmugglerFilesApi();

				smugglerFilesApi.Options.BatchSize = 5;

				await smugglerFilesApi.Between(options);

				Assert.Equal(countOfConfigurations, (await importStore.AsyncFilesCommands.Configuration.GetKeyNamesAsync(0, 200)).Length);

				for (int i = 0; i < 100; i++)
				{
					Assert.NotNull(await importStore.AsyncFilesCommands.Configuration.GetKeyAsync<RavenJObject>("items/" + i));
				}
			}
		}

		[Fact, Trait("Category", "Smuggler")]
		public async Task ExportShouldDisableSynchronizationDestinations()
		{
			using (var exportStream = new MemoryStream())
			using (var exportStore = NewStore())
			using (var importStore = NewStore(1))
			{
				await exportStore.AsyncFilesCommands.Synchronization.SetDestinationsAsync(new SynchronizationDestination()
				{
					ServerUrl = "http://sample.com",
					FileSystem = "Sample",
					Enabled = true
				});

				var exportOptions = new SmugglerExportOptions<FilesConnectionStringOptions>
				{
					From = new FilesConnectionStringOptions
					{
						Url = exportStore.Url,
						DefaultFileSystem = exportStore.DefaultFileSystem
					},
					ToStream = exportStream
				};

				await new SmugglerFilesApi().ExportData(exportOptions);

				exportStream.Position = 0;

				var importOptions = new SmugglerImportOptions<FilesConnectionStringOptions>
				{
					FromStream = exportStream,
					To = new FilesConnectionStringOptions()
					{
						Url = importStore.Url,
						DefaultFileSystem = importStore.DefaultFileSystem
					}
				};

				await new SmugglerFilesApi().ImportData(importOptions);

				var destinations = await importStore.AsyncFilesCommands.Synchronization.GetDestinationsAsync();

				Assert.Equal(1, destinations.Length);
				Assert.Equal("http://sample.com/fs/Sample", destinations[0].Url);
				Assert.Equal("Sample", destinations[0].FileSystem);
				Assert.False(destinations[0].Enabled);
			}
		}

		[Fact, Trait("Category", "Smuggler")]
		public async Task BetweenExportShouldDisableSynchronizationDestinations()
		{
			using (var exportStore = NewStore())
			using (var importStore = NewStore(1))
			{
				await exportStore.AsyncFilesCommands.Synchronization.SetDestinationsAsync(new SynchronizationDestination()
				{
					ServerUrl = "http://sample.com",
					FileSystem = "Sample",
					Enabled = true
				});

				var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>()
				{
					From = new FilesConnectionStringOptions()
					{
						Url = exportStore.Url,
						DefaultFileSystem = exportStore.DefaultFileSystem
					},
					To = new FilesConnectionStringOptions()
					{
						Url = importStore.Url,
						DefaultFileSystem = importStore.DefaultFileSystem
					}
				};

				await new SmugglerFilesApi().Between(options);

				var destinations = await importStore.AsyncFilesCommands.Synchronization.GetDestinationsAsync();

				Assert.Equal(1, destinations.Length);
				Assert.Equal("http://sample.com/fs/Sample", destinations[0].Url);
				Assert.Equal("Sample", destinations[0].FileSystem);
				Assert.False(destinations[0].Enabled);
			}
		}
	}
}