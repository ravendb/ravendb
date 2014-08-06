using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util.Streams;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Server.RavenFS.Storage.Voron.Impl;
using Raven.Tests.Helpers;
using Xunit;
using Voron;
using Voron.Debugging;
using System.IO;

namespace Raven.Tests.Issues
{
	public class IndexResestWithReplication : RavenTestBase
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "Replication; Compression";
		}

		[Fact]
		public void Reset_index_in_database_with_replication_should_not_corrupt_etag_indice()
		{
			var sb = new StringBuilder();
			for (int i = 0; i < 1000; i++)
				sb.Append(Guid.NewGuid());
			var aLongString = sb.ToString();

			using (var store = NewDocumentStore())
			{
				var ravenDocumentsByEntityName = new RavenDocumentsByEntityName();
				ravenDocumentsByEntityName.Execute(store);

				using (var operation = store.BulkInsert(options: new BulkInsertOptions
				{
					BatchSize = 1
				}))
				{

					for (int i = 0; i < 10; i++)
						operation.Store(new {Foo = aLongString}, "Foo/" + i);
				}
				WaitForIndexing(store);

					using (var session = store.OpenSession())
					{
						var count = session.Query<dynamic>().Count();
						Assert.Equal(10, count);
					}

					store.SystemDatabase.Indexes.ResetIndex(ravenDocumentsByEntityName.IndexName);

					WaitForIndexing(store);

					using (var session = store.OpenSession())
					{
						var count = session.Query<dynamic>().Count();
						var errors = store.DatabaseCommands.GetStatistics().Errors;
						Assert.Empty(errors);
						Assert.Equal(10, count);
					}
				

			}
		}

//		[Fact]
//		public void Reset_index_from_debug_journal_should_not_corrupt_database()
//		{
//			var lines = File.ReadAllLines("failed_index_repro.djrs", Encoding.UTF8);
//			var badLines = lines.Where(x => x.Count(c => c == ',') != 3).ToList();
//			Assert.Empty(badLines);
//
//			using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
//			{
//				env.DebugJournal = DebugJournal.FromFile("failed_index_repro", env);
//				env.DebugJournal.Replay();
//
//				using (var tableStorage = new Raven.Database.Storage.Voron.Impl.TableStorage(env, new BufferPool(1024 * 1024 * 1024, 1024 * 32)))
//				{
//					var documentNumberFromCount = tableStorage.GetEntriesCount(tableStorage.Documents);
//					var documentNumberFromIndex = tableStorage.GetEntriesCount(tableStorage.IndexingMetadata);					
//				}
//			}
//		}
	}
}
