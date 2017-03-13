using System;
using System.Diagnostics;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine(i);
                using (var a = new SlowTests.Issues.RavenDB934())
                {
                    a.HighLevelExportsByDocPrefixRemote();
                }
            }
        }
    }
}