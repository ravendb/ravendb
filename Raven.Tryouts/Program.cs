using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;

namespace Raven.Tryouts
{
    public class Customer
    {
        public string Region;
        public string Id;
    }

    public class Invoice
    {
        public string Customer;
    }
	
    public class Program
    {
        private static void Main()
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "t2"
            }.Initialize())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        bulkInsert.Store(new Customer
                        {
                            Region = "regions/" +i%100
                        });
                    }
                }

                Console.WriteLine("Done");
                Console.ReadLine();
            }
        }

    }
}