using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.FileSystem;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.MailingList;

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
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"_", new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "Shop"}}, //existing data
                {"ME", new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "Shop_ME"}},
                {"US", new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "Shop_US"}},
            };

            var shardStrategy = new ShardStrategy(shards)
                .ShardingOn<Customer>(c => c.Region)
                .ShardingOn<Invoice>(i => i.Customer);

            var x = new ShardedDocumentStore(shardStrategy).Initialize();
            using (var s = x.OpenSession())
            {
                var customer = new Customer
                {
                    Region = "US"
                };
                s.Store(customer);
                s.Store(new Invoice
                {
                    Customer = customer.Id
                });
                s.SaveChanges();
            }
        }

    }
}