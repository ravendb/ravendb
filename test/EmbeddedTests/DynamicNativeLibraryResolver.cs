using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace EmbeddedTests
{
#if NETCOREAPP3_1_OR_GREATER
    internal static class DynamicNativeLibraryResolver
    {
        public static void Register(Assembly asm, string lib)
        {
            NativeLibrary.SetDllImportResolver(asm, Resolver);
        }

        private static IntPtr Resolver(string libraryName, Assembly assembly, DllImportSearchPath? dllImportSearchPath)
        {
            return NativeLibrary.Load($"runtimes\\{RuntimeInformation.RuntimeIdentifier}\\native\\{libraryName}", assembly, dllImportSearchPath);
        }
    }
#endif
}
