using System;

using FastTests.Server.Documents.Indexing;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (var i = 0; i < 100000; i++)
            {
                Console.WriteLine(i);

                using (var x = new BasicIndexing())
                {
                    x.SimpleIndexing();
                }
            }
        }
    }
}
