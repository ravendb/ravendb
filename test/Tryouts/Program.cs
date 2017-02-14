using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                using (var a = new FastTests.Server.Documents.Indexing.LiveIndexingPerformanceCollectorTests())
                {
                    a.CanObtainLiveIndexingPerformanceStats().Wait();
                }
            }
        }
    }
}