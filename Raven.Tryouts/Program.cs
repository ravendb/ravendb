using System;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Extensions;
using System.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			using(var store = new DocumentStore
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

				var sp = Stopwatch.StartNew();
				while (true)
				{
					var statistics = store.DatabaseCommands.GetStatistics();
					if (statistics.StaleIndexes.Length == 0)
						break;

					Console.Write("\r                                                                     \r");

					foreach (var stat in statistics.Indexes.Where(x => statistics.StaleIndexes.Contains(x.Name)))
					{
						Console.Write("{0}: {1:#,#}  ", stat.Name, stat.IndexingAttempts);
					}

					Console.Write(sp.Elapsed);
					Thread.Sleep(1000);
				}

				Console.WriteLine(sp.Elapsed);
			}
		}
	}
}