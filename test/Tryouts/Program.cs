using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            const string name = "stackoverflow";
            var doc = MultiDatabase.CreateDatabaseDocument(name);

            using (var store = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = name })
            {
                store.Initialize();

                store.Admin.Send(new CreateDatabaseOperation(doc));
            }
        }
    }
}