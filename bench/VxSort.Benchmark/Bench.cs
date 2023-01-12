using System;
using System.Drawing;
using System.Runtime.CompilerServices;
using Bench.Utils;
using BenchmarkDotNet.Running;
using Microsoft.VisualBasic.CompilerServices;

namespace Bench
{
    class Program
    {
        static unsafe void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
