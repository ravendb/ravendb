using System;
using System.Threading.Tasks;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var test = new SlowTests.Cluster.ClusterTransactionTests())
                {
                    await test.CanCreateClusterTransactionRequest();
                }
            }
        }
    }
}
