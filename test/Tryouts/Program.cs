using System.Linq;
using FastTests.Server.Documents.Replication;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Linq;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        public static void Main(string[] args)
        {
            var store = new DocumentStore()
            {
                Url = "http://localhost.fiddler:8080",
                DefaultDatabase = "Temp"
            };
            store.Initialize();
            /*store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
            {
                Id = "Temp",
                Settings = { { "Raven/DataDir", @"~\Databases\Temp"} }
            });
            using (var session = store.OpenSession())
            {
                session.Store(new User()
                {
                    Name = "Iftah"
                }, "users/1");
                session.Store(new User()
                {
                    Name = "Idan"
                }, "users/2");
                session.SaveChanges();
            };*/

            using (var session = store.OpenNewSession())
            {
                var q = session.Query<User>()
                    .Where(x => x.Name.Equals("Iftah"))
                    .ToList();
            };
            store.Dispose();
        }
    }
}