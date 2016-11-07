using System;
using System.Diagnostics;
using FastTests.Issues;
using FastTests.Sparrow;
using FastTests.Voron.Bugs;

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
                using (var a = new RavenDB_5570())
                {
                    a.Doing_PUT_without_commit_should_not_cause_NRE_on_subsequent_PUTs();
                }
                Console.WriteLine(sp.Elapsed);
            }
        }
    }

}

