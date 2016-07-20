using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Indexing.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var mapReduce = new MapReduceBench(seed: 1))
            {
                mapReduce.Execute();
            }
        }
    }
}
