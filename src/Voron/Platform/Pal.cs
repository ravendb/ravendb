using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Platform;

// ReSharper disable SwitchStatementMissingSomeCases
// ReSharper disable InconsistentNaming

namespace Voron.Platform
{
    public static unsafe class Pal
    {
        static Pal()
        {
            var toFilename = LIBRVNPAL;
            string fromFilename;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                if (RuntimeInformation.ProcessArchitecture != Architecture.Arm &&
                    RuntimeInformation.ProcessArchitecture != Architecture.Arm64)
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.linux.x64.so" : $"{toFilename}.linux.x86.so";
                    toFilename += ".so";
                }
                else
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.arm.64.so" : $"{toFilename}.arm.32.so";
                    toFilename += ".so";
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.mac.x64.so" : $"{toFilename}.mac.x86.so";
                // in mac we are not : `toFilename += ".so";` as DllImport doesn't assume .so nor .dylib by default
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
        public static extern Int32 rvn_write_header(
            string filename,
            void* header,
            Int32 size,
            out UInt32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_get_error_string(
            UInt32 errorCode,
            void* sb,
            Int32 capacity,
            out Int32 specialErrnoCodes
            );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 open_journal(
            string file_name,
            Int32 mode,
            Int64 file_size,
            out IntPtr handle,
            out UInt32 error_code);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 close_journal(
            IntPtr handle,
            out UInt32 errorCode
        );


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 write_journal(
            IntPtr handle,
            IntPtr buffer,
            UInt64 size,
            Int64 offset,
            out UInt32 errorCode
        );
    }
}
