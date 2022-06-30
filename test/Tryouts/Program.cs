using System;
using System.Diagnostics;
using Tests.Infrastructure;
using FastTests.Voron.Sets;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static void Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        for (int i = 0; i < 100; i++)
        {
            Console.WriteLine($"Starting to run {i}");
            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                {
                    new SetTests(testOutputHelper).CanDeleteAndInsertInRandomOrder(73014, 35);
                    //new SetAddRemoval(testOutputHelper).AdditionsAndRemovalWork();

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
                            new SetTests(testOutputHelper).CanDeleteAndInsertInRandomOrder(number, seed);
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
                return;
            }
        }
    }
}
