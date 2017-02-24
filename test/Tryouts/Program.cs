using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client.Attachments;
using Lucene.Net.Store;
using Sparrow.Utils;
using Voron;
using Directory = System.IO.Directory;

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