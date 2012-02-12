using System;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			DateTime? xx1 = null;
			DateTime? xx2 = DateTime.MinValue;

			Console.WriteLine(xx1 > xx2);
			return;

			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
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

				new RavenDocumentsByEntityName().Execute(store);

				var sp = Stopwatch.StartNew();
				while (true)
				{
					var statistics = store.DatabaseCommands.GetStatistics();
					if (statistics.StaleIndexes.Length == 0)
						break;

					Console.Clear();
					foreach (var stat in statistics.Indexes.Where(x => statistics.StaleIndexes.Contains(x.Name)))
					{
						Console.WriteLine("{0}: {1:#,#}  ", stat.Name, stat.IndexingAttempts);
					}

					Console.WriteLine("{0:#,#}",statistics.CurrentNumberOfItemsToIndexInSingleBatch);
					Console.Write(sp.Elapsed);
					Thread.Sleep(2500);
				}

				Console.WriteLine(sp.Elapsed);
			}
		}
	}
}