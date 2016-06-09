using System;
using FastTests.Server.Documents.Expiration;

namespace Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var x = new SlowTests.Core.Querying.Filtering())
                {
                    x.BasicFiltering().Wait();
                }

            }

        }
    }
}