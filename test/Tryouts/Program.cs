using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
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

        private const int numOfItems = 100;

        public static void Main(string[] args)
        {
            var users = new List<User>(numOfItems);
            Console.Write("Creating test data...");
            for(int i = 0; i < numOfItems; i++)
                users.Add(new User
                {
                    FirstName = $"First Name - {i}",
                    LastName = $"Last Name - {i}"
                });
            Console.WriteLine("done");

            using (var store = new DocumentStore
            {
                Url = "http://localhost.fiddler:8080",
                DefaultDatabase = "FooBar123"				
            })
            {
                store.Initialize();
                store.DatabaseCommands.GlobalAdmin.DeleteDatabase("FooBar123",true);
                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "FooBar123",
                    Settings =
                    {
                        { "Raven/DataDir", "~\\FooBar123" }
                    }
                });

                var sw = Stopwatch.StartNew();
                AsyncHelpers.RunSync(() => BulkInsert(store,users));
                Console.WriteLine($"Elapsed : {sw.ElapsedMilliseconds} ms");
            }
        }

        public static async Task BulkInsert(DocumentStore store, List<User> users)
        {
            Console.Write("Opening bulk-insert...");
            using (var bulkInsert = store.BulkInsert())
            {
                Console.WriteLine("done");
                int id = 1;
                foreach (var user in users)
                    await bulkInsert.StoreAsync(user, $"users/{id++}");
                Console.Write("Closing bulk-insert...");
            }
            Console.WriteLine("done");
        }
    }
}
