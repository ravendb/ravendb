using System;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Server;
using Raven.Http;

namespace Raven.Tryouts
{
    class Program
    {
        static void Main()
        {
        	var db = new DocumentDatabase(new RavenConfiguration
        	                              	{
        	                              		DataDirectory = @"C:\Users\Ayende\Downloads\Data",
												IndexSingleThreaded = true
        	                              	});
        	var ravenDbHttpServer = new RavenDbHttpServer (db.Configuration, db);
			ravenDbHttpServer.Start();
        	Console.WriteLine("Started...");
        	new TaskExecuter(db.TransactionalStorage, db.WorkContext).Execute();
        	Console.WriteLine("Done");
        }
    }
}
