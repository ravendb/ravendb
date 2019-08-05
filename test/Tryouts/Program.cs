using System;
using Lucene.Net.Util;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Tests.Faceted;
using StressTests.Cluster;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            long max=-1, min=int.MaxValue, sum = 0, iterations = 5, warmup = 1;
            for (int i = 0; i < iterations; i++)
            {
                if(i%10 == 0)
                    Console.WriteLine(i);
                //Console.WriteLine($"Test run #{i}");
                try
                {
                    using (var test = new DynamicFacets())
                    {
                        var time = test.ProfileFacet();
                        if (i >= warmup)
                        {
                            if (time > max)
                            {
                                max = time;
                            }

                            if (min > time)
                            {
                                min = time;
                            }

                            sum += time;
                        }
                        
                        Console.WriteLine($"query time = {time}ms");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

            }
            Console.WriteLine($"max={max} min={min} avg={(int)(sum / (iterations - warmup))}");
        }
    }
}
