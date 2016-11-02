using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Json;

namespace Voron.Benchmark
{
    public class Configuration
    {
        public const int RecordsPerTransaction = 1000;
        public const int Transactions = 1000;
        public const string Path = @"C:\Scratch\bench.data";

        /// <summary>
        /// This is the global allocator for test strings
        /// </summary>
        public static readonly ByteStringContext Allocator;

        /// <summary>
        /// Initialization has to be done this way to avoid breaking when
        /// multithreading
        /// </summary>
        static Configuration()
        {
            Allocator = new ByteStringContext();
        }
    }
}
