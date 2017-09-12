using System;
using FastTests.Client;
using FastTests.Smuggler;
using SlowTests.Core.AdminConsole;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Server.Replication;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var test = new RavenDB_6711_RavenEtl())
            {
                try
                {
                    test.Script_defined_for_all_documents_with_filtering_and_loads_to_the_same_collection_for_some_docs();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.WriteLine("-------------");
                    throw;
                }
            }
        }
    }
}
