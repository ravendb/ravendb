using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;

// ReSharper disable SwitchStatementMissingSomeCases
// ReSharper disable InconsistentNaming

namespace Voron.Platform
{
    public static unsafe class Pal
    {
        static Pal()
        {
            var toFilename = LIBRVNPAL;
            var fromFilename = $"{toFilename}.win.x64.dll";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.linux.x64.so" : $"{toFilename}.linux.x86.so";
                toFilename += ".so";
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.mac.x64.dylib" : $"{toFilename}.mac.x86.dylib";
                toFilename += ".dylib";
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.win.x64.dll" : $"{toFilename}.win.x86.dll";
                toFilename += ".dll";
            }
            else
            {
                throw new NotSupportedException("Not supported platform - no Linux/OSX/Windows is detected ");
            }

            try
            {
                if (File.Exists(toFilename))
                    return;

                File.Move(fromFilename, toFilename);
            }
            catch (IOException e)
            {
                throw new IOException(
                    $"Cannot copy {fromFilename} to {toFilename}, make sure appropriate {toFilename} to your platform architecture exists in Raven.Server executable folder",
                    e);
            }
        }
        
        [SuppressMessage("ReSharper", "InconsistentNaming")]
        private const string LIBRVNPAL = "librvnpal";

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern int rvn_write_header(
            [MarshalAs(UnmanagedType.LPStr)] string filename,
            void* header,
            [MarshalAs(UnmanagedType.U4)] uint size,
            ref PalFlags.Errno failCodes);
    }
}
