using System;
using System.Threading;
using Lucene.Net.Store;
using Raven.Database;
using Raven.Database.Indexing;
using Raven.Database.Server.Responders;
using Raven.Database.Storage.SchemaUpdates;
using Statistics = Raven.Tests.Indexes.Statistics;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			
			try
			{
				var ravenConfiguration = new RavenConfiguration
				{
					DataDirectory = @"C:\Work\StackOverflow.Data"
				};
				using(var db = new DocumentDatabase(ravenConfiguration))
				{
					db.TransactionalStorage.Batch(actions =>
					{
						actions.TEST();
					});


					db.PutIndex("Raven/DocumentsByEntityName",
					            new IndexDefinition
					            {
					            	Map =
					            		@"from doc in docs 
where doc[""@metadata""][""Raven-Entity-Name""] != null 
select new { Tag = doc[""@metadata""][""Raven-Entity-Name""] };
",
					            	Indexes = {{"Tag", FieldIndexing.NotAnalyzed}},
					            	Stores = {{"Tag", FieldStorage.No}}
					            });

					
					Console.WriteLine(db.ApproximateTaskCount);
					db.SpinBackgroundWorkers();
					do
					{
						Thread.Sleep(500);
						Console.WriteLine(db.ApproximateTaskCount);
					} while (db.HasTasks);
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}

}