using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System.Linq;
using Raven.Json.Linq;
using Raven.Tests.Bundles.PeriodicBackups;
using Raven.Tests.Bundles.Replication.Bugs;
using Raven.Tests.Bundles.Versioning;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var masterdocs = new List<string>();

			var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();

			int i = 0;
			using(var writer = new StreamWriter("master.txt"))
			using (var streamDocs = store.DatabaseCommands.ForDatabase("TheMaster").StreamDocs())
			{
				while (streamDocs.MoveNext())
				{
					var id = streamDocs.Current.Value<RavenJObject>("@metadata").Value<string>("@id");
					if(i++ % 1000 == 0)
						Console.WriteLine(id);
					writer.WriteLine(id);
				}
				writer.Flush();
			}

			
		}
	}
}