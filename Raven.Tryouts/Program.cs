using System;
using System.Diagnostics;
using System.IO;
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
                Url = "http://127.0.0.1:8080",
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

                BulkInsert(store, 5);
                using (var file = File.CreateText("results.csv"))
                {
                    file.WriteLine("Count,ElapsedMs");
                    for (int count = 1; count < 1024 * 128; count += (1024 * 4))
                    {
                        var sw = Stopwatch.StartNew();
                        BulkInsert(store, count);
                        file.WriteLine(String.Format("{0},{1}", count, sw.ElapsedMilliseconds));
                        Console.WriteLine(count);
                    }
                    file.Flush();
                }
            }
        }

        static int id = 1;
        public static void BulkInsert(DocumentStore store, int numOfItems)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < numOfItems; i++)
                    bulkInsert.Store(new User
                    {
                        FirstName = String.Format("First Name - {0}", i),
                        LastName = String.Format("Last Name - {0}", i)
                    }, String.Format("users/{0}", id++));
            }
        }
    }
}