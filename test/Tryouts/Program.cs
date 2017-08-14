using System;
using FastTests.Server;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.Queries.Dynamic.Map;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;
using Raven.Server.Documents.Replication;
using Raven.Client.Documents;
using SlowTests.Client.Subscriptions;
using SlowTests.Tests.Linq;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.Clear();
                Console.WriteLine(i);
                using (var test = new SlowTests.Server.Documents.ETL.Raven.RavenDB_6711_RavenEtl())
                {
                    try
                    {
                        test.Script_defined_for_all_documents_with_filtering_and_loads_to_the_same_collection_for_some_docs();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.Beep();
                        return;
                    }
                }
            }
        }
    }
}
