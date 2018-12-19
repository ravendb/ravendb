// -----------------------------------------------------------------------
//  <copyright file="Win32Helper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Utils;
using Voron.Impl.FileHeaders;
using Voron.Util.Settings;

namespace Voron.Platform.Win32
{
    public class Win32Helper
    {
        public static IntPtr CurrentProcess = Win32NativeMethods.GetCurrentProcess();

        public static unsafe bool TryReadFileHeader(FileHeader* header, VoronPathSetting path)
        {
            using (var fs = SafeFileStream.Create(path.FullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.None))
            {
                if (fs.Length != sizeof(FileHeader))
                    return false; // wrong file size

                var ptr = (byte*)header;
                int remaining = sizeof(FileHeader);
                while (remaining > 0)
                {
                    int read;
                    if (Win32NativeFileMethods.ReadFile(fs.SafeFileHandle, ptr, remaining, out read, null) == false)
                        throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to read file " + path);
                    if (read == 0)
                        return false; // we should be reading _something_ here, if we can't, then it is an error and we assume corruption
                    ptr += read;
                    remaining -= read;
                }
                return true;
            }
        }

         
    }
}
