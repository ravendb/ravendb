using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Benchmark
{
    public class Program
    {
        private static HashSet<long> _randomNumbers;


        public static void Main()
        {
#if DEBUG
            var oldfg = Console.ForegroundColor;
            var oldbg = Console.BackgroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = ConsoleColor.Yellow;

            Console.WriteLine("Dude! Are you seriously running a benchmark in debug mode?!");
            Console.ForegroundColor = oldfg;
            Console.BackgroundColor = oldbg;
#endif
            _randomNumbers = InitRandomNumbers(Configuration.Transactions * Configuration.ItemsPerTransaction);

            var prefixTreeBench = new PrefixTreeBench(_randomNumbers);
            prefixTreeBench.Execute();

            var btreeBench = new BTreeBench(_randomNumbers);
            btreeBench.Execute();
        }

        private static HashSet<long> InitRandomNumbers(int count)
        {
            var random = new Random(1337 ^ 13);
            var randomNumbers = new HashSet<long>();
            while (randomNumbers.Count < count)
            {
                randomNumbers.Add(random.Next(0, int.MaxValue));
            }
            return randomNumbers;
        }
    }
}
