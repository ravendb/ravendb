using System;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Voron.Issues;
using StressTests.Cluster;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 123; i++)
            {
                Console.WriteLine(i);
                try
                {
                    using (var test = new RavenDB_10825())
                    {
                        test.Encryption_buffer_of_freed_scratch_page_must_not_affect_another_overflow_allocation_on_tx_commit();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }
    }
}
