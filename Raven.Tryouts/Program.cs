using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Document;
using Raven.Tests.Faceted;
using Raven.Tests.Issues;
using System.Linq;
using Raven.Tests.Util;
using Xunit;
using Version = Lucene.Net.Util.Version;

namespace Raven.Tryouts
{
	internal class Program
	{
		[STAThread]
		private static void Main()
		{
			var x = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "bulk"
			}.Initialize();

			var sp = Stopwatch.StartNew();
			using(var remoteBulkInsertOperation = new RemoteBulkInsertOperation((ServerClient) x.DatabaseCommands,1024*8))
			{
				for (int i = 0; i < 250*1000; i++)
				{
					remoteBulkInsertOperation.Write(Guid.NewGuid().ToString(), new RavenJObject{{Constants.RavenEntityName, "Tests"}},
						new RavenJObject{{"Age", i*2}});
					if(i % 1000 == 0)
						Console.WriteLine(i);
				}
			}
			Console.WriteLine(sp.Elapsed);

			x.Dispose();
		}
	}
}