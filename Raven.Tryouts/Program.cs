using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Tests.Bugs;
using System.Linq;
using Raven.Tests.Faceted;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			var documentDatabase = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = @"C:\Work\ravendb-1.2\Raven.Server\bin\Debug\Data\Databases\Imdb",
				MaxNumberOfParallelIndexTasks = 1
			});

			Task.Factory.StartNew(() =>
			{
				while (true)
				{
					Thread.Sleep(1000);
					var indexStats = documentDatabase.Statistics.Indexes.First(x => x.Name.StartsWith("Raven/") == false);
					var indexingPerformanceStats = indexStats.Performance.LastOrDefault();
					
					Console.Clear();
					Console.WriteLine("{0}: {1:#,#} - {2}", indexStats.Name, indexStats.IndexingAttempts, indexStats.LastIndexedEtag);
					if(indexingPerformanceStats != null)
					{
						Console.WriteLine("{1:#,#} - {0}", indexingPerformanceStats.Duration, indexingPerformanceStats.InputCount);
					}
				}
			});

			var indexingExecuter = new IndexingExecuter(documentDatabase.WorkContext);

			indexingExecuter.Execute();
		}
	}
}