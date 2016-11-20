using System;
using FastTests.Server.Documents.Replication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, "E:\\Work");
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var store = new FastTests.Server.Documents.Indexing.Static.BasicStaticMapReduceIndexing())
                {
                    store.Static_map_reduce_index_with_multiple_outputs_per_document().Wait();
                }
            }
        }
    }

}

