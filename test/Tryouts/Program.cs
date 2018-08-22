using System;
using System.IO;
using System.Threading.Tasks;
using RachisTests;
using RachisTests.DatabaseCluster;
using Raven.Client.Exceptions.Database;
using SlowTests.Server.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var fullPath = Path.Join(Path.GetTempPath(),$"{Guid.NewGuid()}");
            LoggingSource.Instance.SetupLogMode(LogMode.Information, fullPath);            
            Console.WriteLine(fullPath);
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                try
                {
                    using (var test = new TopologyChangesTests())
                    {
                        await test.AddingRemovedNodeShouldWork();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.ReadKey();
                }           
            }
        }
    }
}
