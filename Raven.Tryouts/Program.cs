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
        	                              		DataDirectory = @"C:\Users\Ayende\Downloads\Nick.Data",
												IndexSingleThreaded = true,
												AnonymousUserAccessMode = AnonymousUserAccessMode.All
        	                              	});
        	var ravenDbHttpServer = new RavenDbHttpServer (db.Configuration, db);
			ravenDbHttpServer.Start();
        	Console.WriteLine("Started...");
        	new TasksExecuter(db.TransactionalStorage, db.WorkContext).Execute();
        	Console.WriteLine("Done");
        }
    }
}
