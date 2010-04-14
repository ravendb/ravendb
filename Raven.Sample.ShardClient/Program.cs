using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Client;

namespace Raven.Sample.ShardClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var shards = new Shards { 
                new DocumentStore("localhost", 8080) { Identifier="Shard1" }, 
                new DocumentStore("localhost", 8081) { Identifier="Shard2" } 
            };

            using (var documentStore = new ShardedDocumentStore(new ShardStrategy(), shards).Initialise())
            using (var session = documentStore.OpenSession())
            {
                //store 2 items in the 2 shards
            	session.Store(new Company {Name = "Company 1", Region = "A"});
            	session.Store(new Company {Name = "Company 2", Region = "B"});

                //get all, should automagically retrieve from each shard
                var allCompanies = session.Query<Company>().ToArray();

                foreach(var company in allCompanies)
                    Console.WriteLine(company.Name);
            }
        }
    }
}