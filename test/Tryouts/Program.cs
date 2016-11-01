using System;
using System.Diagnostics;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                var sp = Stopwatch.StartNew();
                var x = new FastTests.Voron.Trees.CanDefrag();
                {
                    x.CanDeleteAtRoot();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

