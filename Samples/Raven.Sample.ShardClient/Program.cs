using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Client;

namespace Raven.Sample.ShardClient
{
    class Program
    {
        static void Main()
        {
            var shards = new Shards
            {
                CreateShard("Asia", "http://localhost:8080"),
                CreateShard("Middle-East", "http://localhost:8081"),
            };

            using (var documentStore = new ShardedDocumentStore(new ShardStrategy(), shards).Initialize())
            using (var session = documentStore.OpenSession())
            {
                //store 2 items in the 2 shards
                session.Store(new Company { Name = "Company 1", Region = "Asia" });
                session.Store(new Company { Name = "Company 2", Region = "Middle East" });
                session.SaveChanges();

                //get all, should automagically retrieve from each shard
                var allCompanies = session.Advanced.LuceneQuery<Company>()
                    .WaitForNonStaleResults().ToArray();

                foreach (var company in allCompanies)
                    Console.WriteLine(company.Name);
            }
        }

        private static DocumentStore CreateShard(string identifier, string url)
        {
            var documentStore = new DocumentStore
            {
                Identifier = identifier,
                Url = url,
            };

            return documentStore;
        }
    }
}
