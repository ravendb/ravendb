using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Benchmark
{
    public class Configuration
    {
        public const int ItemsPerTransaction = 1000;
        public const int Transactions = 500;
        public const string Path = @"D:\scratch\bench.data";
    }
}
