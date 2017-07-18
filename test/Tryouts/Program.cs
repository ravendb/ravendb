using System;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {   
                Console.WriteLine(i);
                using (var test = new RavenDB_6886())   
                {
                    test.Cluster_identity_for_single_document_in_parallel_on_different_nodes_should_work().Wait();
                }
            }
        }
    }
}
