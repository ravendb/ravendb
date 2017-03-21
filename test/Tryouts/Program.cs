using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine(i);

                using (var a = new FastTests.Server.Replication.ReplicationIndexesAndTransformers())
                {
                    a.Manually_removed_indexes_would_remove_metadata_on_startup().Wait();
                }
                
            }
        }
    }
}