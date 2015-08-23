using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
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

		private const string apikey = "test/ThisIsMySecret";

		[Fact]
		public async Task SmugglerExport_and_import_with_ApiKey_should_work()
		{
			using (var serverA = GetNewServer(port: 8010,configureConfig: ConfigureServerForAuth))
			using (var serverB = GetNewServer(port: 8011, configureConfig: ConfigureServerForAuth))
			using (var ravenStoreA = NewRemoteDocumentStore(ravenDbServer: serverA))
			using (var ravenStoreB = NewRemoteDocumentStore(ravenDbServer: serverB))
			using (var counterStoreA = NewRemoteCountersStore("storeX",ravenStore: ravenStoreA))
			using (var counterStoreB = NewRemoteCountersStore("storeY", ravenStore: ravenStoreB))
			{								
				ConfigureApiKey(serverA.SystemDatabase, "test", "ThisIsMySecret", counterStoreA.Name);
				ConfigureApiKey(serverB.SystemDatabase, "test", "ThisIsMySecret", counterStoreB.Name);

				await counterStoreA.IncrementAsync("G", "C");
				await counterStoreA.DecrementAsync("G", "C2");

				var smugglerApi = new SmugglerCounterApi();

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename,
					From = ConnectionStringTo(counterStoreA)
				});

				//Not finished yet
			}			
		}

		[Fact]
		public void SmugglerExport_with_error_in_stream_should_fail_gracefully()
		{
			using (var counterStore = NewRemoteCountersStore("store"))
			using (var stream = new FailingStream())
			{
				var smugglerApi = new SmugglerCounterApi();

				this.Invoking(x => AsyncHelpers.RunSync(() => smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToStream = stream,
					From = ConnectionStringTo(counterStore)
				}))).ShouldThrow<FailingStreamException>();
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

				this.Invoking(x => AsyncHelpers.RunSync(() => smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
				{
					FromStream = stream,
					To = new CounterConnectionStringOptions
					{
						Url = counterStore.Url,
						CounterStoreId = counterStore.Name
					}
				}))).ShouldThrow<FailingStreamException>();
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
				fileInfo.Exists.Should().BeTrue();
				fileInfo.Length.Should().BeGreaterThan(0);
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

				incrementalFolder.Exists.Should().BeTrue();
				var dumpFiles = incrementalFolder.GetFiles();
				dumpFiles.Should().HaveCount(3);
				dumpFiles.Should().OnlyContain(x => x.Length > 0);
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

				summary.Should().HaveCount(3); //sanity check

				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.GroupName == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c2" && x.GroupName == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c" && x.GroupName == "g");

				summary.First(x => x.CounterName == "c1" && x.GroupName == "g1").Total.Should().Be(5);
				summary.First(x => x.CounterName == "c2" && x.GroupName == "g1").Total.Should().Be(3);
				summary.First(x => x.CounterName == "c" && x.GroupName == "g").Total.Should().Be(-2);
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

				summary.Should().HaveCount(3); //sanity check
				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.GroupName == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c2" && x.GroupName == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.GroupName == "g2");

				summary.First(x => x.CounterName == "c1" && x.GroupName == "g1").Total.Should().Be(6); //change + inc
				summary.First(x => x.CounterName == "c2" && x.GroupName == "g1").Total.Should().Be(1);
				summary.First(x => x.CounterName == "c1" && x.GroupName == "g2").Total.Should().Be(-1);
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

				summary.Should().HaveCount(3); //sanity check
				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.GroupName == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c2" && x.GroupName == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.GroupName == "g2");

				summary.First(x => x.CounterName == "c1" && x.GroupName == "g1").Total.Should().Be(2); 
				summary.First(x => x.CounterName == "c2" && x.GroupName == "g1").Total.Should().Be(1);
				summary.First(x => x.CounterName == "c1" && x.GroupName == "g2").Total.Should().Be(4);
			}
		}

		private CounterConnectionStringOptions ConnectionStringTo(ICounterStore counterStore)
		{
			return new CounterConnectionStringOptions
			{
				ApiKey = counterStore.Credentials.ApiKey,
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
