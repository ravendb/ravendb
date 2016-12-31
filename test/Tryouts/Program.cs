using System;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                
                Console.WriteLine(i);
                using (var a = new FastTests.Voron.Trees.Basic())
                {
                    a.CanAddEnoughToCausePageSplit();
                }
            }
        }
    }

}

