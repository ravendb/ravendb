using System;
using System.Data.SqlClient;
using System.Diagnostics;
using FastTests.Server;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.Queries.Dynamic.Map;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;
using Raven.Server.Documents.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using SlowTests.Tests.Linq;
using Sparrow.Json;
using SQLToRavenMigration;

namespace Tryouts
{
    public class Program
    {
        public void NotifyPerTable(string tableName)
        {
            Console.WriteLine($"'{tableName}' table has been written.");
        }

        public void NotifyDocuments(int documentsCount)
        {
            Console.WriteLine($"Documents count: {documentsCount}");
        }
        public static void Main(string[] args)
        {
            var doc = new 
            {
                Array = new byte[] {1, 2, 3, 4, 5}
            };
            var context = JsonOperationContext.ShortTermSingleUse();
            EntityToBlittable.ConvertEntityToBlittable(doc, new DocumentConventions(), context, new DocumentInfo
            {
                Collection = "foo"
            });


            
        }


    }
}
