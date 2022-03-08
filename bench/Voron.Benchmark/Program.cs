using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Benchmark.BTree;
using BenchmarkDotNet.Running;
using Constants = Voron.Global.Constants;
using BenchmarkDotNet.Configs;

namespace Voron.Benchmark
{
    public class Program
    {
#if DEBUG
        static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new DebugInProcessConfig());
#else
        static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
#endif
    }
}
