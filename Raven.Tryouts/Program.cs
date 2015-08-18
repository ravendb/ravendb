using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Storage.Voron.Impl;

namespace ConsoleApplication4
{
    class Program
    {
        public class Item
        {
            public int Number;
        }
        private static void Main(string[] args)
        {
            var ds = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "mr"
            }.Initialize();

            using (var bulk = ds.BulkInsert())
            {
                for (int i = 0; i < 1000 * 1000; i++)
                {
                    bulk.Store(new Item { Number = 1 });
                }
            }

        }

    }

    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }

        public string Phone { get; set; }
        public string Fax { get; set; }
    }
}
