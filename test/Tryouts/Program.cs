using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        public static void Main(string[] args)
        {

            using (var store = new DocumentStore
            {
                Url = "http://127.0.0.1:8081",
                DefaultDatabase = "FooBar123"
            })
            {
                store.Initialize();
                store.DatabaseCommands.GlobalAdmin.DeleteDatabase("FooBar123", true);
                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "FooBar123",
                    Settings =
                    {
                        { "Raven/DataDir", "~\\FooBar123" }
                    }
                });

                BulkInsert(store, 1024 * 1024 * 10).Wait();
            }
        }

        public static async Task BulkInsert(DocumentStore store, int numOfItems)
        {
            var sp = Stopwatch.StartNew();
            using (var bulkInsert = store.BulkInsert())
            {
                int id = 1;
                for (int i = 0; i < numOfItems; i++)
                    await bulkInsert.StoreAsync(new User
                    {
                        FirstName = $"First Name - {i}",
                        LastName = $"Last Name - {i}"
                    }, $"users/{id++}");
            }
            Console.WriteLine(sp.ElapsedMilliseconds);
        }
    }
}