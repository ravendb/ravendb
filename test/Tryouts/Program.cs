using System;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.Write(i);
                var sp = Stopwatch.StartNew();
               
                using (var x = new MapReduce_IndependentSteps())
                {
                    x.CanGetReducedValues();
                }
                Console.WriteLine(" - " + sp.Elapsed);
            }
        }
    }

}

