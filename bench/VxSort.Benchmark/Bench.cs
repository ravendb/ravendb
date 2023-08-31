using BenchmarkDotNet.Running;

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
