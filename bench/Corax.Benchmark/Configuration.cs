using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Corax.Benchmark
{
    public class Configuration
    {
        public const int ItemsPerTransaction = 400;
        public const int Transactions = 500;

        public const string WikipediaDir = @"D:\Scratch\Wikipedia";
        public const string Path = @"d:\scratch\bench.data";
    }
}
