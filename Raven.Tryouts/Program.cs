using System;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
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
			var json = JObject.Parse(File.ReadAllText(@"C:\Users\Ayende\Downloads\RavenBugRepro\before.txt"));

			DocumentStore store = new DocumentStore { Url = "http://localhost:8080" };
			store.Initialize();

			using (var session = store.OpenSession())
			{
				session.Store(json);
				session.SaveChanges();

				// Here you can see that the document was saved successfully
				var loadedDoc = session.Advanced.LuceneQuery<dynamic>().Last();
				Console.WriteLine(loadedDoc);
			}

			using (var session = store.OpenSession())
			{
				// Here you will see all-empty values
				var loadedDoc = session.Advanced.LuceneQuery<dynamic>().Last();
				Console.WriteLine(loadedDoc);
			}
        }
    }
}
