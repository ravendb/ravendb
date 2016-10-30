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
                var x = new FastTests.Blittable.BlittableJsonEqualityTests();
                {
                    x.Equals_even_though_order_of_properties_is_different();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

