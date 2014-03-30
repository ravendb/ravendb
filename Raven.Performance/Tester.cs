using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.Tar;
using NLog;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Extensions;

namespace Raven.Performance
{
	public class Tester
	{
		private readonly string dataLocation;
		private readonly DocumentStore store;
		public List<long> MemoryUsage { get; set; }
        public List<KeyValuePair<string, double>> LatencyTimes { get; set; }
		public List<KeyValuePair<string, long>> LatencyInDocuments { get; set; }

		private const int BatchSize = 512;
		private readonly Logger logger = LogManager.GetLogger("log");

		private volatile bool doneImporting;
		private Process process;

		public Tester(string dataLocation, Process process)
		{
			this.process = process;
			this.dataLocation = dataLocation;

			MemoryUsage = new List<long>();
            LatencyTimes = new List<KeyValuePair<string, double>>();
            LatencyInDocuments = new List<KeyValuePair<string, long>>();

			store = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "HibernatingRhinos.TestsDatabase"
			};
			store.Initialize();

		}

		public void ClearDatabase(string serverLocation)
		{
			var databaseLocation = Path.Combine(serverLocation, "Tenants", "HibernatingRhinos.TestsDatabase");
			IOExtensions.DeleteDirectory(databaseLocation);
		}

		public void AddData()
		{
			MemoryUsage.Clear();
			doneImporting = false;
			var session = store.OpenSession();
			var count = 0;

			logger.Info("Testing RavenDB Log");

			var sp = ParseDisks(diskToAdd =>
			{
				session.Store(diskToAdd);
				count += 1;
				if (count < BatchSize)
					return;

				session.SaveChanges();
				session = store.OpenSession();
				count = 0;
			});

			session.SaveChanges();

			logger.Info(" ");
			logger.Info("Done in {0}", sp.Elapsed);
			doneImporting = true;

		}

		private Stopwatch ParseDisks(Action<Disk> addToBatch)
		{
			int i = 0;
			var parser = new Parser();
			var buffer = new byte[1024 * 1024];// more than big enough for all files

			var sp = Stopwatch.StartNew();

			using (var bz2 = new BZip2InputStream(File.Open(dataLocation, FileMode.Open)))
			using (var tar = new TarInputStream(bz2))
			{
				TarEntry entry;
				while ((entry = tar.GetNextEntry()) != null)
				{
					if (entry.Size == 0 || entry.Name == "README" || entry.Name == "COPYING")
						continue;
					var readSoFar = 0;
					while (true)
					{
						var read = tar.Read(buffer, readSoFar, ((int)entry.Size) - readSoFar);
						if (read == 0)
							break;

						readSoFar += read;
					}
					// we do it in this fashion to have the stream reader detect the BOM / unicode / other stuff
					// so we can read the values properly
					var fileText = new StreamReader(new MemoryStream(buffer, 0, readSoFar)).ReadToEnd();
					try
					{
						var disk = parser.Parse(fileText);
						addToBatch(disk);
						if (i++ % BatchSize == 0)
						{
							process.Refresh();
							MemoryUsage.Add(process.WorkingSet64);
							logger.Info("\r{0} {1:#,#} {2} ", entry.Name, i, sp.Elapsed);
						}
					}
					catch (Exception e)
					{
						logger.Error("");
						logger.Error(entry.Name);
						logger.Error(e);
						return sp;
					}
				}
			}
			return sp;
		}

		public void CreateAllIndexes()
		{
			CreateIndexEntityName();
			CreateIndexSearch();
			CreateIndexSimple();
		}

		public void CreateIndexSimple()
		{
			store.DatabaseCommands.PutIndex("Disks/Simple", new IndexDefinition
			{
				Map =
					@"
from disk in docs.Disks
select new
{
	disk.Artist,
	disk.Title
}"
			});
		}

		public void CreateIndexSearch()
		{
			store.DatabaseCommands.PutIndex("Disks/Search", new IndexDefinition
			{
				Map =
					@"
from disk in docs.Disks
select new
{
	Query = new[] { disk.Artist, disk.Title },
	disk.Tracks,
	DisId = disk.DiskIds
}",
				Indexes =
						{
							{"Query", FieldIndexing.Analyzed},
							{"Tracks", FieldIndexing.Analyzed}
						}
			});
		}

		public void CreateIndexEntityName()
		{
			new RavenDocumentsByEntityName().Execute(store);
		}

		public void WaitForIndexesToBecomeNonStale()
		{
			MemoryUsage.Clear();
			while (true)
			{
				process.Refresh();
				MemoryUsage.Add(process.WorkingSet64);

				var statistics = store.DatabaseCommands.GetStatistics();

				if (statistics.StaleIndexes.Length == 0 && doneImporting)
					return;

				foreach (var staleIndex in statistics.StaleIndexes)
				{
					var indexStats = statistics.Indexes.Single(x => x.PublicName == staleIndex);
					var latencyInTime = (DateTime.UtcNow - indexStats.LastIndexedTimestamp).TotalMilliseconds;
					LatencyTimes.Add(new KeyValuePair<string, double>(staleIndex, latencyInTime));

					var latencyInDocuments = statistics.CountOfDocuments - indexStats.IndexingAttempts;
                    LatencyInDocuments.Add(new KeyValuePair<string, long>(staleIndex, latencyInDocuments));

					logger.Debug("Stale index {0} - {1:#,#}/{2:#,#} - latency: {3:#,#}, {4:#,#}ms", indexStats.Id, indexStats.IndexingAttempts, statistics.CountOfDocuments,
						latencyInDocuments,
						latencyInTime);
				}

				Thread.Sleep(1000);
			}
		}

		public Task AddDataAsync()
		{
			doneImporting = false;
			return Task.Factory.StartNew(AddData);
		}

		public IEnumerable<Tuple<string, IEnumerable<double>, IEnumerable<long>>> Latencies()
		{
			foreach (var item in LatencyTimes.GroupBy(x => x.Key))
			{
				yield return
					Tuple.Create(item.Key, 
                        item.Select(x => x.Value),
                         LatencyInDocuments.Where(pair => pair.Key == item.Key).Select(x => x.Value));
			}
		}
	}
}