using System;
<<<<<<< HEAD
using System.Diagnostics;
=======
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
>>>>>>> e077157fdb5a06ba352144146283f9253e2a60bc
using System.Threading.Tasks;
using FastTests.Client.BulkInsert;
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

            public string[] Tags { get; set; }
        }

        public static void Main(string[] args)
        {
            //using (var x = new BulkInserts())
            //{
            //    x.SimpleBulkInsertShouldWork().Wait();
            //}
            //return;
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

<<<<<<< HEAD
                BulkInsert(store, 1024 * 1024 * 10).Wait();
=======
                BulkInsert(store, 1024  *512).Wait();
>>>>>>> e077157fdb5a06ba352144146283f9253e2a60bc
            }
        }

        public static async Task BulkInsert(DocumentStore store, int numOfItems)
        {
<<<<<<< HEAD
            var sp = Stopwatch.StartNew();
=======
            Console.Write("Doing bulk-insert...");

            string[] tags = null;// Enumerable.Range(0, 1024*8).Select(x => "Tags i" + x).ToArray();

            var sp = System.Diagnostics.Stopwatch.StartNew();
>>>>>>> e077157fdb5a06ba352144146283f9253e2a60bc
            using (var bulkInsert = store.BulkInsert())
            {
                int id = 1;
                for (int i = 0; i < numOfItems; i++)
                    await bulkInsert.StoreAsync(new User
                    {
                        FirstName = $"First Name - {i}",
                        LastName = $"Last Name - {i}",
                        Tags = tags
                    }, $"users/{id++}");
            }
<<<<<<< HEAD
            Console.WriteLine(sp.ElapsedMilliseconds);
=======
            Console.WriteLine("done in " + sp.Elapsed);
>>>>>>> e077157fdb5a06ba352144146283f9253e2a60bc
        }
    }
}