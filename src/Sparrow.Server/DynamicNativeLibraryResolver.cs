using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Sparrow.Server
{
    public static class DynamicNativeLibraryResolver
    {
        private static Dictionary<string, Func<string, string>> _registered = new Dictionary<string, Func<string, string>>();
        public static void Register(string lib, Func<string, string> mutator = null)
        {
            lock (typeof(DynamicNativeLibraryResolver))
            {
                var copy = new Dictionary<string, Func<string, string>>(_registered)
                {
                    [lib] = mutator
                };
                _registered = copy;
                if (copy.Count != 1)
                    return;
                NativeLibrary.SetDllImportResolver(typeof(Sodium).Assembly, Resolver);
            }
        }

        private static IntPtr Resolver(string libraryName, Assembly assembly, DllImportSearchPath? dllImportSearchPath)
        {
            if(_registered.TryGetValue(libraryName, out var mutator) == false)
                return IntPtr.Zero;

            string suffix;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                if (RuntimeInformation.ProcessArchitecture != Architecture.Arm &&
                    RuntimeInformation.ProcessArchitecture != Architecture.Arm64)
                {
                    suffix = Environment.Is64BitProcess ? ".linux.x64.so" : ".linux.x86.so";
                }
                else
                {
                    suffix = Environment.Is64BitProcess ? ".arm.64.so" : ".arm.32.so";
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                suffix = Environment.Is64BitProcess ? ".mac.x64.dylib" : ".mac.x86.dylib";
                // in mac we are not : `toFilename += ".so";` as DllImport doesn't assume .so nor .dylib by default
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                suffix = Environment.Is64BitProcess ? ".win.x64.dll" : ".win.x86.dll";
            }
            else
            {
                throw new NotSupportedException("Non supported platform - no Linux/OSX/Windows is detected ");
            }

            var name = libraryName + suffix;
            if (mutator != null)
                name = mutator(name);
            return NativeLibrary.Load(name, assembly, dllImportSearchPath);
        }
    }
}
