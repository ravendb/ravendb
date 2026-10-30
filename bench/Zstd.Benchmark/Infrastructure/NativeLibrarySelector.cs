using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Utils;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Points the production binding (<see cref="ZstdLib"/>) at a specific libzstd binary, so one BenchmarkDotNet run
    /// can compare library versions side by side: every job runs in its own process with a different environment variable.
    /// </summary>
    internal static class NativeLibrarySelector
    {
        public const string LibraryPathVariable = "RAVEN_ZSTD_BENCH_LIB";
        public const string ExpectedVersionVariable = "RAVEN_ZSTD_BENCH_EXPECTED_VERSION";

        private static readonly object Locker = new();
        private static ZstdNativeLibrary _current;
        private static bool _resolverRegistered;

        public static string SelectedPath { get; private set; }

        [ModuleInitializer]
        internal static void Initialize()
        {
            string path = Environment.GetEnvironmentVariable(LibraryPathVariable);
            if (string.IsNullOrEmpty(path) == false)
                Select(path);
        }

        /// <summary>
        /// Must run before the first P/Invoke into libzstd made through <see cref="ZstdLib"/>.
        /// </summary>
        public static void Select(string path)
        {
            path = Path.GetFullPath(path);
            if (File.Exists(path) == false)
                throw new FileNotFoundException($"libzstd binary not found: {path}", path);

            lock (Locker)
            {
                SelectedPath = path;

                // Sparrow leaves libzstd to the .NET host (RavenDB.Zstd.Native package), so the resolver for its assembly is free to take
                if (_resolverRegistered == false)
                {
                    NativeLibrary.SetDllImportResolver(typeof(ZstdLib).Assembly, (name, assembly, searchPath) =>
                        name == ZstdLib.LIBZSTD && SelectedPath != null ? NativeLibrary.Load(SelectedPath) : IntPtr.Zero);
                    _resolverRegistered = true;
                }
            }
        }

        /// <summary>
        /// The libzstd from the RavenDB.Zstd.Native package: next to the application in a RID specific build, under runtimes/ otherwise.
        /// </summary>
        public static string DefaultLibraryPath
        {
            get
            {
                string flat = Path.Combine(AppContext.BaseDirectory, LibraryFileName);
                if (File.Exists(flat))
                    return flat;

                return Path.Combine(AppContext.BaseDirectory, "runtimes", RuntimeId, "native", LibraryFileName);
            }
        }

        /// <summary>
        /// The binary the production binding is using in this process. Forces the production binding to load first,
        /// then opens the same file by full path (the OS hands back the already loaded module).
        /// </summary>
        public static ZstdNativeLibrary Current
        {
            get
            {
                if (_current != null)
                    return _current;

                lock (Locker)
                {
                    if (_current != null)
                        return _current;

                    ZstdLib.GetMaxCompression(1);
                    _current = new ZstdNativeLibrary(SelectedPath ?? DefaultLibraryPath);
                    return _current;
                }
            }
        }

        /// <summary>
        /// Called from every benchmark's setup. Prints which binary is in use and fails the run if it is not the one the host asked for.
        /// </summary>
        public static void EnsureExpectedLibrary()
        {
            ZstdNativeLibrary current = Current;
            Console.WriteLine($"// [zstd-bench] {current.Describe()}");

            string expected = Environment.GetEnvironmentVariable(ExpectedVersionVariable);
            if (string.IsNullOrEmpty(expected) == false && string.Equals(expected, current.Version, StringComparison.Ordinal) == false)
                throw new InvalidOperationException($"Expected libzstd {expected} but {current.Version} was loaded from {current.Path}");
        }

        private static string LibraryFileName
        {
            get
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    return "libzstd.dll";
                if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                    return "libzstd.dylib";
                return "libzstd.so";
            }
        }

        private static string RuntimeId
        {
            get
            {
                string os;
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    os = "win";
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    os = "linux";
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                    os = "osx";
                else
                    throw new PlatformNotSupportedException();

                string arch = RuntimeInformation.ProcessArchitecture switch
                {
                    Architecture.X64 => "x64",
                    Architecture.X86 => "x86",
                    Architecture.Arm64 => "arm64",
                    Architecture.Arm => "arm",
                    _ => throw new PlatformNotSupportedException()
                };

                return $"{os}-{arch}";
            }
        }
    }
}
