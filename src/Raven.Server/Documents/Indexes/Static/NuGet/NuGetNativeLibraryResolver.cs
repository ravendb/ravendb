using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Raven.Server.Utils;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public static class NuGetNativeLibraryResolver
    {
        private static readonly object _locker = new object();

        private static readonly HashSet<string> _registeredPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private static readonly Dictionary<Assembly, Reference<bool>> _registeredAssemblies = new Dictionary<Assembly, Reference<bool>>();

        private static readonly Dictionary<string, string> _nativeLibraries = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public static void RegisterPath(string path)
        {
            if (path == null)
                return;

            if (Directory.Exists(path) == false)
                return;

            lock (_locker)
            {
                if (_registeredPaths.Add(path) == false)
                    return;

                foreach (var libraryPath in Directory.GetFiles(path))
                {
                    if (IsNativeLibrary(libraryPath) == false)
                        continue;

                    var nativeLibrary = Path.GetFileNameWithoutExtension(libraryPath);
                    _nativeLibraries[nativeLibrary] = libraryPath;
                }
            }
        }

        public static void RegisterAssembly(Assembly assembly)
        {
            if (assembly == null)
                return;

            lock (_locker)
            {
                _registeredAssemblies.TryAdd(assembly, new Reference<bool>());
            }
        }

        public static void EnsureAssembliesRegisteredToNativeLibraries()
        {
            lock (_locker)
            {
                if (_nativeLibraries.Count == 0 || _registeredAssemblies.Count == 0)
                    return;

                foreach (var kvp in _registeredAssemblies)
                {
                    var assembly = kvp.Key;
                    var registered = kvp.Value;

                    if (registered.Value)
                        continue;

                    NativeLibrary.SetDllImportResolver(assembly, Resolver);
                    registered.Value = true;
                }
            }
        }

        private static IntPtr Resolver(string libraryName, Assembly assembly, DllImportSearchPath? dllImportSearchPath)
        {
            if (_nativeLibraries.TryGetValue(libraryName, out var libraryPath) == false)
            {
                if (PlatformDetails.RunningOnPosix)
                {
                    libraryName = $"lib{libraryName}";
                    if (_nativeLibraries.TryGetValue(libraryName, out libraryPath) == false)
                        return IntPtr.Zero;
                }
                else
                    return IntPtr.Zero;
            }

            if (File.Exists(libraryPath) == false)
                return IntPtr.Zero;

            return NativeLibrary.Load(libraryPath);
        }

        private static bool IsNativeLibrary(string libraryPath)
        {
            if (string.IsNullOrWhiteSpace(libraryPath))
                return false;

            var extension = Path.GetExtension(libraryPath);
            if (string.IsNullOrWhiteSpace(extension))
                return false;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return string.Equals(".so", extension, StringComparison.OrdinalIgnoreCase);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return string.Equals(".dylib", extension, StringComparison.OrdinalIgnoreCase);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return string.Equals(".dll", extension, StringComparison.OrdinalIgnoreCase);

            return false;
        }
    }
}
