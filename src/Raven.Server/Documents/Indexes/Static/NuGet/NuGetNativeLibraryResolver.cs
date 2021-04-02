using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public static class NuGetNativeLibraryResolver
    {
        private const uint LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000;

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        static extern bool SetDefaultDllDirectories(uint flags);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern int AddDllDirectory(string path);

        private static readonly object _locker = new object();

        private static readonly HashSet<string> _registeredPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public static void RegisterPath(string path)
        {
            if (path == null)
                return;

            if (Directory.Exists(path) == false)
                return;

            lock (_locker)
            {
                if (_registeredPaths.Count == 0)
                    SetDefaultDllDirectories(LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);

                if (_registeredPaths.Add(path) == false)
                    return;

                var result = AddDllDirectory(path);
            }
        }
    }
}
