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
			DocumentStore store = new DocumentStore { Url = "http://localhost:8080" };
			store.Initialize();

			using (var session = store.OpenSession())
			{
				session.Store(new{Name = "Ayende"});
				session.SaveChanges();

			}

        }
    }
}
