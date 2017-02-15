using System;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Parallel.For(0, 1000, i =>
            {
                Console.Write(".");

                using (var a = new FastTests.Server.Replication.ReplicationResolveToDatabase())
                {
                    a.UnsetDatabaseResolver();
                }
            });

            //for(int i = 0; i < 10000; i++)
            //{
            //    Console.WriteLine(i);

            //    using (var a = new MaxSecondsForTaskToWaitForDatabaseToLoad())
            //    {
            //        a.Should_throw_when_there_is_timeout();
            //    }
            //}
        }
    }
}