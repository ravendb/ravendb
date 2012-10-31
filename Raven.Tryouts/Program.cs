using System;
using System.Collections.Generic;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Write("\r" + i);
				Environment.SetEnvironmentVariable("Run", i.ToString());
				using (var x = new MultiOutputReduce())
				{
					x.CanGetCorrectResultsFromAllItems();
				}
			}

			//using(var docDb = new DocumentDatabase(new RavenConfiguration
			//{
			//	RunInMemory = true
			//}))
			//{
			//	docDb.PutIndex("My", new IndexDefinition
			//	{
			//		Map = "from doc in docs.Docs select new { doc.Name }"
			//	});

			//	docDb.Put("Raven/Hilo/docs", null, new RavenJObject{{"Max", 32}}, new RavenJObject(), null);


			//	docDb.Batch(new ICommandData[]
			//	{
			//		new PutCommandData
			//		{
			//			Key = "docs/1",
			//			Metadata = new RavenJObject{{Constants.RavenEntityName, "Docs"}},
			//			Document = new RavenJObject{{"Name", "oren"}}
			//		},
			//		new PutCommandData
			//		{
			//			Key = "docs/2",
			//			Metadata = new RavenJObject{{Constants.RavenEntityName, "Docs"}},
			//			Document = new RavenJObject{{"Name", "ayende"}}
			//		},  
			//	});

			//	var jsonDocuments = docDb.IndexingExecuter.GetJsonDocuments(Guid.Empty);
			//	jsonDocuments = docDb.IndexingExecuter.GetJsonDocuments(Guid.Parse("00000000-0000-0100-0000-000000000002"));
			//}
		}
	}
}