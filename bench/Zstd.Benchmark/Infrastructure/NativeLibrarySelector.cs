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

            // ZstdLib's static constructor registers the default mapping, run it first so it does not overwrite ours
            RuntimeHelpers.RunClassConstructor(typeof(ZstdLib).TypeHandle);
            DynamicNativeLibraryResolver.Register(typeof(ZstdLib).Assembly, "libzstd", _ => path);
            SelectedPath = path;
        }

        public static string DefaultLibraryPath => Path.Combine(AppContext.BaseDirectory, "libzstd" + PlatformSuffix);

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

        private static string PlatformSuffix
        {
            get
            {
                bool isArm = RuntimeInformation.ProcessArchitecture is Architecture.Arm or Architecture.Arm64;
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64)
                        return ".win.arm64.dll";
                    return Environment.Is64BitProcess ? ".win.x64.dll" : ".win.x86.dll";
                }
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    return isArm ? (Environment.Is64BitProcess ? ".arm.64.so" : ".arm.32.so") : ".linux.x64.so";
                if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                    return isArm ? ".mac.arm64.dylib" : ".mac.x64.dylib";

                throw new PlatformNotSupportedException();
            }
        }
    }
}
