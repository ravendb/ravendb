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
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "test2"				
            })
            {
                store.Initialize();
              
                BulkInsert(store).Wait();
            }
        }

        public static async Task BulkInsert(DocumentStore store)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                {
                    await bulkInsert.StoreAsync(new User { FirstName = "foo", LastName = "bar" });
                }
            }
            Console.Write("Opening bulk-insert...");
            var sw = Stopwatch.StartNew();
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 100*1000; i++)
                {
                    await bulkInsert.StoreAsync(new User {FirstName = "foo", LastName = "bar"});
                }
            }
            Console.WriteLine($"Elapsed : {sw.ElapsedMilliseconds} ms");

        }
    }
}
