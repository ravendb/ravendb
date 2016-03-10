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

        private const int numOfItems = 100000;

        public static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "FooBar123"
            })
            {
                store.Initialize();

//				store.DatabaseCommands.GlobalAdmin.DeleteDatabase("FooBar123", true);
//				store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
//				{
//					Id = "FooBar123",
//					Settings =
//					{
//						{ "Raven/DataDir", "~\\FooBar123" }
//					}
//				});

                BulkInsert(store,100).Wait();
            }
        }

        public static async Task BulkInsert(DocumentStore store,int sizeBytes)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                {
                    await bulkInsert.StoreAsync(new User { FirstName = "foo", LastName = "bar" });
                }
            }
            Console.Write($"Doing bulk-insert with docs sized -> {sizeBytes} bytes...");
            var sw = Stopwatch.StartNew();
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < numOfItems; i++)
                {
                    await bulkInsert.StoreAsync(new User {FirstName = "foo", LastName = "bar"});
                }
            }
            Console.WriteLine($"Elapsed : {sw.ElapsedMilliseconds} ms");

        }
    }
}
