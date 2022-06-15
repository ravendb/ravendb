using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using FastTests.Client.Subscriptions;
using FastTests.Voron;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        using (var testOutputHelper = new ConsoleTestOutputHelper())
            new CompactTreeTests(testOutputHelper).CanDeleteLargeNumberOfItemsInRandomInsertionOrder(60597, 54632);

        Console.WriteLine(Process.GetCurrentProcess().Id);
        for (int i = 0; i < 100; i++)
        {
            Console.WriteLine($"Starting to run {i}");
            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                {                    
                    int minFailure = int.MaxValue;
                    int failureRandom = -1;                    

                    var rnd = new Random();
                    int number = 500000;
                    while (number > 16)
                    {
                        int seed = rnd.Next(100000);
                        try
                        {
                            //new CompactTreeTests(testOutputHelper).CanDeleteLargeNumberOfItemsInRandomInsertionOrder(2023, 13878);
                            new CompactTreeTests(testOutputHelper).CanDeleteLargeNumberOfItemsInRandomInsertionOrder(number, seed);
                        }
                        catch (Exception ex)
                        {
                            if (number < minFailure)
                            {
                                minFailure = number;
                                failureRandom = seed;
                                Console.WriteLine($"[N:{minFailure}, Rnd:{failureRandom}]");
                                Console.WriteLine($"--> {ex}");
                            }
                        }

                        number = rnd.Next(Math.Min(500000, minFailure));
                    }

                    Console.ReadLine();
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }
}
