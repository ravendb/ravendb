using System;
using System.Diagnostics;
using FastTests.Sparrow;

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
                var x = new SlowTests.Voron.MultiAdds();
                x.MultiAdds_And_MultiDeletes_After_Causing_PageSplit_DoNot_Fail(500);
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

