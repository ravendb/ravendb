using System;
using SlowTests.Core.Indexing;
using SlowTests.SlowTests.Bugs;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);

                using (var x = new TimeoutTester())
                {
                    x.will_timeout_query_after_some_time();
                }
            }
        }
    }
}

