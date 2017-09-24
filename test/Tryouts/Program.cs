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
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new SlowTests.Issues.RavenDB_6886())
                {
                    try
                    {
                        test.Cluster_identity_for_single_document_should_work().Wait();
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
}
