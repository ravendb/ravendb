using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Xunit;

namespace Raven.Tests.Counters
{
	public class SmugglerTests : RavenBaseCountersTest
	{
		private const string CounterDumpFilename = "testCounter.counterdump";

		public SmugglerTests()
		{
			IOExtensions.DeleteFile(CounterDumpFilename);
			IOExtensions.DeleteDirectory(CounterDumpFilename); //counters incremental export creates folder with incremental dump files
		}

		private const string goodApikey = "test/ThisIsMySecret";
		private const string badApikey = "test2/ThisIsNotMySecret";

		[Fact]
		public async Task Smuggler_export_with_ApiKey_should_work()
		{
			using (var server = GetNewServer(port: 8010,configureConfig: ConfigureServerForAuth))
			using (var documentStore = NewRemoteDocumentStore(ravenDbServer: server))
			{
				using (var counterStore = NewRemoteCountersStore("storeX", ravenStore: documentStore))
				{
					await counterStore.IncrementAsync("G", "C");
					await counterStore.DecrementAsync("G", "C2");
				}

				using (var counterStore = NewRemoteCountersStore("storeX", ravenStore: documentStore))
				{
					ConfigureApiKey(server.SystemDatabase, "test", "ThisIsMySecret", counterStore.Name, true);
					var smugglerApi = new SmugglerCounterApi();

					Assert.DoesNotThrow(() => AsyncHelpers.RunSync(() =>
						smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
						{
							ToFile = CounterDumpFilename,
							From = ConnectionStringTo(counterStore, goodApikey)
						})));
				}
			}
		}

		//note: smuggler import/export requires admin access to <system> database
		[Fact]
		public async Task Smuggler_import_with_ApiKey_should_work()
		{
			using (var serverA = GetNewServer(port: 8010,configureConfig: ConfigureServerForAuth))
			using (var serverB = GetNewServer(port: 8011, configureConfig: ConfigureServerForAuth))
			using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA))
			using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
			{
				using (var counterStoreA = NewRemoteCountersStore("storeX", ravenStore: ravenStoreA))
				{
					await counterStoreA.IncrementAsync("G", "C");
					await counterStoreA.DecrementAsync("G", "C2");
				}

				using (var counterStoreA = NewRemoteCountersStore("storeX", ravenStore: ravenStoreA))
				using (var counterStoreB = NewRemoteCountersStore("storeY", ravenStore: ravenStoreB))
				{
					ConfigureApiKey(serverA.SystemDatabase, "test", "ThisIsMySecret", counterStoreA.Name, true);
					ConfigureApiKey(serverA.SystemDatabase, "test2", "ThisIsNotMySecret", counterStoreA.Name + "FooBar", true);
					ConfigureApiKey(serverB.SystemDatabase, "test", "ThisIsMySecret", counterStoreB.Name, true);
					ConfigureApiKey(serverB.SystemDatabase, "test2", "ThisIsNotMySecret", counterStoreB.Name + "FooBar", true);

					var smugglerApi = new SmugglerCounterApi();

					var e = Assert.Throws<ErrorResponseException>(() => AsyncHelpers.RunSync(() =>
						smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
						{
							ToFile = CounterDumpFilename,
							From = ConnectionStringTo(counterStoreA, badApikey)
						})));

                    Assert.Equal(HttpStatusCode.Forbidden, e.StatusCode);
					

					Assert.DoesNotThrow(() => AsyncHelpers.RunSync(() =>
						smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
						{
							ToFile = CounterDumpFilename,
							From = ConnectionStringTo(counterStoreA, goodApikey)
						})));

					e = Assert.Throws<ErrorResponseException>(() => AsyncHelpers.RunSync(() =>
						smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
						{
							FromFile = CounterDumpFilename,
							To = ConnectionStringTo(counterStoreB, badApikey)
						})));

                    Assert.Equal(HttpStatusCode.Forbidden, e.StatusCode);

                    Assert.DoesNotThrow(() => AsyncHelpers.RunSync(() =>
						smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
						{
							FromFile = CounterDumpFilename,
							To = ConnectionStringTo(counterStoreB, goodApikey)
						})));
				}
			}
		}

		[Fact]
		public void SmugglerExport_with_error_in_stream_should_fail_gracefully()
		{
			using (var counterStore = NewRemoteCountersStore("store"))
			using (var stream = new FailingStream())
			{
				var smugglerApi = new SmugglerCounterApi();

				Assert.Throws<FailingStreamException>(() => AsyncHelpers.RunSync(() => smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToStream = stream,
					From = ConnectionStringTo(counterStore)
				})));
			}
		}

		//make sure that if a stream throws exception during import it comes through
		[Fact]
		public void SmugglerImport_with_error_in_stream_should_fail_gracefully()
		{
			using (var counterStore = NewRemoteCountersStore("store"))
			using (var stream = new FailingStream())
			{
				var smugglerApi = new SmugglerCounterApi();

                Assert.Throws<FailingStreamException>(() => AsyncHelpers.RunSync(() => smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
				{
					FromStream = stream,
					To = new CounterConnectionStringOptions
					{
						Url = counterStore.Url,
						CounterStoreId = counterStore.Name
					}
				})));
			}
		}

		[Fact]
		public async Task SmugglerExport_to_file_should_not_fail()
		{
			using (var counterStore = NewRemoteCountersStore("store"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c1");
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.IncrementAsync("g2", "c1");

				var smugglerApi = new SmugglerCounterApi();
				
				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});

				var fileInfo = new FileInfo(CounterDumpFilename);
				Assert.True(fileInfo.Exists);
				Assert.True(fileInfo.Length > 0);
			}
		}

		[Fact]
		public async Task SmugglerExport_incremental_to_file_should_not_fail()
		{
			using (var counterStore = NewRemoteCountersStore("store"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c1");
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.IncrementAsync("g2", "c1");

				var smugglerApi = new SmugglerCounterApi();
				smugglerApi.Options.Incremental = true;
				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});
				
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.DecrementAsync("g2", "c1");

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});

				var incrementalFolder = new DirectoryInfo(CounterDumpFilename);

				Assert.True(incrementalFolder.Exists);
				var dumpFiles = incrementalFolder.GetFiles();
                Assert.Equal(3, dumpFiles.Length);
                Assert.True(dumpFiles.All(x=>x.Length>0));
			}			
		}

		[Fact]
		public async Task SmugglerImport_incremental_from_file_should_work()
		{
			using (var counterStore = NewRemoteCountersStore("storeToExport"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c2");

				var smugglerApi = new SmugglerCounterApi
				{
					Options = { Incremental = true }
				};

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});

				await counterStore.IncrementAsync("g", "c");
				await counterStore.IncrementAsync("g1", "c2");				

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});

				await counterStore.ChangeAsync("g", "c", -3);

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});
			}

			using (var counterStore = NewRemoteCountersStore("storeToImportTo"))
			{
				var smugglerApi = new SmugglerCounterApi()
				{
					Options = {Incremental = true}
				};

				await smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
				{
					FromFile = CounterDumpFilename,
					To = ConnectionStringTo(counterStore)
				});

				var summary = await counterStore.Admin.GetCounterStorageSummary(counterStore.Name);
                Assert.Equal(3, summary.Length);//sanity check
                Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c1" && x.GroupName == "g1"));
                Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c2" && x.GroupName == "g1"));
                Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c" && x.GroupName == "g"));

                Assert.Equal(5, summary.First(x => x.CounterName == "c1" && x.GroupName == "g1").Total);
                Assert.Equal(3, summary.First(x => x.CounterName == "c2" && x.GroupName == "g1").Total);
                Assert.Equal(-2, summary.First(x => x.CounterName == "c" && x.GroupName == "g").Total);
			}
		}

		[Fact]
		public async Task SmugglerImport_from_file_should_work()
		{
			using (var counterStore = NewRemoteCountersStore("storeToExport"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c1");
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.DecrementAsync("g2", "c1");

				var smugglerApi = new SmugglerCounterApi();

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStore)
				});
			}

			using (var counterStore = NewRemoteCountersStore("storeToImportTo"))
			{
				var smugglerApi = new SmugglerCounterApi();

				await smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
				{
					FromFile = CounterDumpFilename,
					To = ConnectionStringTo(counterStore)
				});

				var summary = await counterStore.Admin.GetCounterStorageSummary(counterStore.Name);
                Assert.Equal(3, summary.Length); //sanity check
                Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c1" && x.GroupName == "g1"));
			    Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c2" && x.GroupName == "g1"));
			    Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c1" && x.GroupName == "g2"));

                Assert.Equal(6, summary.First(x => x.CounterName == "c1" && x.GroupName == "g1").Total);//change + inc
                Assert.Equal(1, summary.First(x => x.CounterName == "c2" && x.GroupName == "g1").Total);
                Assert.Equal(-1, summary.First(x => x.CounterName == "c1" && x.GroupName == "g2").Total);
			}
		}

		[Fact]
		public async Task SmugglerBetween_should_work()
		{
			using (var source = NewRemoteCountersStore("source"))
			using (var target = NewRemoteCountersStore("target"))
			{
				await source.ChangeAsync("g1", "c1", 5);
				await source.ChangeAsync("g1", "c1",-3);
				await source.IncrementAsync("g1", "c2");
				await source.ChangeAsync("g2", "c1",4);

				var smugglerApi = new SmugglerCounterApi();
				await smugglerApi.Between(new SmugglerBetweenOptions<CounterConnectionStringOptions>
				{
					From = ConnectionStringTo(source),
					To = ConnectionStringTo(target)
				});

				var summary = await target.Admin.GetCounterStorageSummary(target.Name);
                Assert.Equal(3, summary.Length); //sanity check
				Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c1" && x.GroupName == "g1"));
                Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c2" && x.GroupName == "g1"));
				Assert.NotNull(summary.SingleOrDefault(x => x.CounterName == "c1" && x.GroupName == "g2"));
				
                Assert.Equal(2, summary.First(x => x.CounterName == "c1" && x.GroupName == "g1").Total);
				Assert.Equal(1, summary.First(x => x.CounterName == "c2" && x.GroupName == "g1").Total);
			    Assert.Equal(4, summary.First(x => x.CounterName == "c1" && x.GroupName == "g2").Total);
			}
		}

		private CounterConnectionStringOptions ConnectionStringTo(ICounterStore counterStore, string overrideApiKey = null)
		{
			return new CounterConnectionStringOptions
			{
				ApiKey = overrideApiKey ?? counterStore.Credentials.ApiKey,
				Credentials = counterStore.Credentials.Credentials,
				CounterStoreId = counterStore.Name,
				Url = counterStore.Url
			};
		}

		private class FailingStreamException : Exception
		{
		}

		private class FailingStream : MemoryStream
		{
			public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
			{
				throw new FailingStreamException();
			}

			public override void WriteByte(byte value)
			{
				throw new FailingStreamException();
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new FailingStreamException();
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				throw new FailingStreamException();
			}

			public override int ReadByte()
			{
				throw new FailingStreamException();
			}

			public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
			{
				throw new FailingStreamException();
			}
		}
	}
}
