using System;
using System.Threading.Tasks;
using SlowTests.Core.Indexing;
using SlowTests.SlowTests.Bugs;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Parallel.For(0, 1000, i =>
            {
                using (var x = new Fanout())
                {
                    x.ShouldSkipDocumentsIfMaxIndexOutputsPerDocumentIsExceeded();
                }
                Console.WriteLine(i);
            });
        }
    }
}

