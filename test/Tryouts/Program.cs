using System;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
               
                using (var x = new SlowTests.SlowTests.Issues.RavenDB_2812())
                {
                    x.ShouldProperlyPageResults();
                }
            }
        }
    }

}

