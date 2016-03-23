using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
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

        public static void Main(string[] args)
        {

            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var x = new BasicIndexing())
                    x.Errors();
            }
        }

        public static async Task BulkInsert(DocumentStore store, int numOfItems)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                Console.Write("Doing bulk-insert...");
                int id = 1;
                for (int i = 0; i < numOfItems; i++)
                    await bulkInsert.StoreAsync(new User
                    {
                        FirstName = $"First Name - {i}",
                        LastName = $"Last Name - {i}"
                    }, $"users/{id++}");
                Console.WriteLine("done");
                Console.Write("Closing bulk-insert...");
            }
            Console.WriteLine("done");
        }
    }
}
