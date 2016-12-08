using System;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, "E:\\Work");

            //Parallel.For(0, 1000, i =>
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i); 
                using (var store = new FastTests.Server.Documents.Indexing.Auto.BasicAutoMapIndexing())
                {
                    store.IndexLoadErrorCreatesFaultyInMemoryIndexFakeAndAddsAlert();
                }
            }
            //); 
        }
    }

}

