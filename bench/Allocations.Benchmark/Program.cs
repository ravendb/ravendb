using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace Allocations.Benchmark
{
    // Runs the public methods of the *Scenarios classes, on a new instance for each, and prints what they allocate.
    // The arguments select the scenarios whose "Class.Method" name contains one of them. Run it in Release:
    // the debug-only checks of Voron and of the server allocate too.
    public static class Program
    {
        public static async Task Main(string[] args)
        {
#if DEBUG
            Console.WriteLine("Debug build: the numbers include the allocations of debug-only checks");
#endif
            var scenarios = typeof(Program).Assembly.GetTypes()
                .Where(t => t.IsPublic && t.IsAbstract == false && t.Name.EndsWith("Scenarios", StringComparison.Ordinal))
                .OrderBy(t => t.Name, StringComparer.Ordinal)
                .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).OrderBy(m => m.MetadataToken))
                .Where(m => m.GetParameters().Length == 0 && m.Name is not (nameof(IDisposable.Dispose) or nameof(IAsyncDisposable.DisposeAsync)))
                .Where(m => args.Length == 0 || args.Any(a => $"{m.DeclaringType!.Name}.{m.Name}".Contains(a, StringComparison.OrdinalIgnoreCase)));

            foreach (var method in scenarios)
            {
                Console.WriteLine($"{method.DeclaringType!.Name}.{method.Name}");
                await RunAsync(method);
            }
        }

        private static async Task RunAsync(MethodInfo method)
        {
            var type = method.DeclaringType!;
            var instance = type.GetConstructor([typeof(ITestOutputHelper)]) is { } withOutput
                ? withOutput.Invoke([new ConsoleTestOutputHelper()])
                : Activator.CreateInstance(type);

            try
            {
                if (method.Invoke(instance, null) is Task task)
                    await task;
            }
            finally
            {
                if (instance is IAsyncDisposable asyncDisposable)
                    await asyncDisposable.DisposeAsync();
                else if (instance is IDisposable disposable)
                    disposable.Dispose();
            }
        }
    }
}
