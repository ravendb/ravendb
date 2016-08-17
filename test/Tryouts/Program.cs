using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var f = new FastTests.Server.Documents.Queries.Dynamic.MapReduce.BasicDynamicMapReduceQueries())
                {
                    f.Group_by_does_not_support_custom_equality_comparer().Wait();
                }
            }

        }
    }
}

