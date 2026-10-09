using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Running;
using Zstd.Benchmark.Infrastructure;
using Zstd.Benchmark.Reporting;

namespace Zstd.Benchmark
{
    public static class Program
    {
        private const string Usage = """
            Zstd.Benchmark - measures RavenDB's use of zstd through the production code paths (Sparrow ZstdLib / ZstdStream).

              run [options] [BenchmarkDotNet args]   run benchmarks (default command)
                  --lib <id>=<path>                  libzstd binary to test, repeatable; the first one is the baseline job.
                                                     default: shipped=<output dir>/libzstd.<platform>
                  --affinity pcores|none|<hexmask>   default pcores (pins to performance cores on hybrid CPUs)
                  --launches <n>                     processes per benchmark case (default 2)
                  --iterations <n>                   measured iterations per process (default 15)
                  --warmups <n>                      warmup iterations per process (default 3)
                  --quick                            1 launch, 5 iterations, 1 warmup - smoke runs only
                  --artifacts <dir>                  where BenchmarkDotNet writes results
                  e.g. --filter *Document*           any BenchmarkDotNet argument is passed through (default --filter *)

              report [--lib <path>] [--out <file>] [--no-sweep]
                  compressed sizes, frame overhead, inner stream call counts and a level sweep for one binary

              compare <baseline dir> [<candidate dir>] [--in-run] [--baseline-job <id>] [--candidate-job <id>]
                      [--confirm-with <dir> | --confirm-with "<baseline dir>;<candidate dir>"] [--threshold 0.03] [--out <file>]
                  compares BenchmarkDotNet full JSON reports:
                    two runs                       compare <before> <after>
                    two libraries in one run       compare <dir> --baseline-job shipped --candidate-job 1.5.7
                    frozen vs production code      compare <dir> --in-run
                    old code+lib vs new code+lib   compare <dir> --in-run --baseline-job shipped --candidate-job 1.5.7
                  --confirm-with repeats the comparison on an independent run and only confirms effects both runs agree on

              libinfo [--lib <id>=<path>]...
                  prints version and multithreading support of binaries
            """;

        public static int Main(string[] args)
        {
            string command = args.Length > 0 && args[0].StartsWith("-", StringComparison.Ordinal) == false ? args[0].ToLowerInvariant() : "run";
            List<string> rest = command == "run" && (args.Length == 0 || args[0] != "run") ? args.ToList() : args.Skip(1).ToList();

            switch (command)
            {
                case "run":
                    return Run(rest);
                case "report":
                    return Report(rest);
                case "compare":
                    return Compare(rest);
                case "libinfo":
                    return LibInfo(rest);
                case "help":
                case "--help":
                    Console.WriteLine(Usage);
                    return 0;
                default:
                    Console.Error.WriteLine($"Unknown command '{command}'.");
                    Console.WriteLine(Usage);
                    return 1;
            }
        }

        private static int Run(List<string> args)
        {
            RunOptions options = new();
            List<string> passThrough = new();
            for (int i = 0; i < args.Count; i++)
            {
                switch (args[i])
                {
                    case "--lib":
                        options.Libraries.Add(ParseLibrary(args[++i]));
                        break;
                    case "--affinity":
                        options.Affinity = args[++i];
                        break;
                    case "--launches":
                        options.Launches = int.Parse(args[++i], CultureInfo.InvariantCulture);
                        break;
                    case "--iterations":
                        options.Iterations = int.Parse(args[++i], CultureInfo.InvariantCulture);
                        break;
                    case "--warmups":
                        options.Warmups = int.Parse(args[++i], CultureInfo.InvariantCulture);
                        break;
                    case "--quick":
                        options.Launches = 1;
                        options.Iterations = 5;
                        options.Warmups = 1;
                        break;
                    case "--artifacts":
                        options.ArtifactsPath = Path.GetFullPath(args[++i]);
                        break;
                    default:
                        passThrough.Add(args[i]);
                        break;
                }
            }

            if (options.Libraries.Count == 0)
                options.Libraries.Add(ParseLibrary("shipped=" + NativeLibrarySelector.DefaultLibraryPath));

            foreach (LibraryUnderTest library in options.Libraries)
            {
                ZstdNativeLibrary native = new(library.Path);
                library.Version = native.Version;
                Console.WriteLine($"// [zstd-bench] job '{library.Id}': {native.Describe()}");
            }

            if (options.Libraries.Select(x => x.Id).Distinct(StringComparer.OrdinalIgnoreCase).Count() != options.Libraries.Count)
                throw new ArgumentException("Library ids must be unique");

            if (passThrough.Contains("--filter") == false && passThrough.Contains("-f") == false)
                passThrough.AddRange(new[] { "--filter", "*" });

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(passThrough.ToArray(), BenchmarkConfigFactory.Create(options));
            return 0;
        }

        private static int Report(List<string> args)
        {
            string output = null;
            bool sweep = true;
            for (int i = 0; i < args.Count; i++)
            {
                switch (args[i])
                {
                    case "--lib":
                        NativeLibrarySelector.Select(args[++i]);
                        break;
                    case "--out":
                        output = args[++i];
                        break;
                    case "--no-sweep":
                        sweep = false;
                        break;
                    default:
                        throw new ArgumentException($"Unknown argument '{args[i]}'");
                }
            }

            string report = CompressionReport.Create(sweep);
            Console.WriteLine(report);
            if (output != null)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(output)));
                File.WriteAllText(output, report);
            }

            return 0;
        }

        private static int Compare(List<string> args)
        {
            List<string> positional = new();
            ComparisonOptions options = new();
            string output = null;
            for (int i = 0; i < args.Count; i++)
            {
                switch (args[i])
                {
                    case "--in-run":
                        options.InRun = true;
                        break;
                    case "--threshold":
                        options.Threshold = double.Parse(args[++i], CultureInfo.InvariantCulture);
                        break;
                    case "--out":
                        output = args[++i];
                        break;
                    case "--baseline-job":
                        options.BaselineJob = args[++i];
                        break;
                    case "--candidate-job":
                        options.CandidateJob = args[++i];
                        break;
                    case "--confirm-with":
                        options.ConfirmWith = args[++i];
                        break;
                    default:
                        positional.Add(args[i]);
                        break;
                }
            }

            if (positional.Count == 1)
                positional.Add(positional[0]);

            if (positional.Count != 2)
            {
                Console.WriteLine(Usage);
                return 1;
            }

            string report = ResultsComparer.Compare(positional[0], positional[1], options);
            Console.WriteLine(report);
            if (output != null)
                File.WriteAllText(output, report);
            return 0;
        }

        private static int LibInfo(List<string> args)
        {
            List<LibraryUnderTest> libraries = new();
            for (int i = 0; i < args.Count; i++)
            {
                if (args[i] == "--lib")
                    libraries.Add(ParseLibrary(args[++i]));
            }

            if (libraries.Count == 0)
                libraries.Add(ParseLibrary("shipped=" + NativeLibrarySelector.DefaultLibraryPath));

            foreach (LibraryUnderTest library in libraries)
                Console.WriteLine($"{library.Id}: {new ZstdNativeLibrary(library.Path).Describe()}");
            return 0;
        }

        private static LibraryUnderTest ParseLibrary(string value)
        {
            int separator = value.IndexOf('=');
            if (separator <= 0)
                throw new ArgumentException($"Expected <id>=<path>, got '{value}'");

            return new LibraryUnderTest { Id = value[..separator], Path = Path.GetFullPath(value[(separator + 1)..]) };
        }
    }
}
