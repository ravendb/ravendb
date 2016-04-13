// -----------------------------------------------------------------------
//  <copyright file="Win32Helper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel;
using System.IO;
using Voron.Impl.FileHeaders;

namespace Voron.Platform.Win32
{
    public class Win32Helper
    {
        public static unsafe void WriteFileHeader(FileHeader* header, string path)
        {
            using (var fs = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.None))
            {
                var ptr = (byte*)header;
                int remaining = sizeof(FileHeader);
                while (remaining > 0)
                {
                    int written;
                    if (Win32NativeFileMethods.WriteFile(fs.SafeFileHandle, ptr, remaining, out written, null) == false)
                        throw new Win32Exception();
                    ptr += written;
                    remaining -= written;
                }
                if(Win32NativeFileMethods.FlushFileBuffers(fs.SafeFileHandle)==false)
                    throw new Win32Exception();
            }
        }

        public static unsafe bool TryReadFileHeader(FileHeader* header, string path)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.None))
            {
                if (fs.Length != sizeof(FileHeader))
                    return false; // wrong file size

                var ptr = (byte*)header;
                int remaining = sizeof(FileHeader);
                while (remaining > 0)
                {
                    int read;
                    if (Win32NativeFileMethods.ReadFile(fs.SafeFileHandle, ptr, remaining, out read, null) == false)
                        throw new Win32Exception();
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
