using System;
using System.ComponentModel;
using System.IO;
using Raven.Server.Exceptions;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Utils;
using Voron.Platform.Win32;

namespace Raven.Server.Utils
{
    public static class ExceptionHelper
    {
        private const int ERROR_COMMITMENT_LIMIT = 1455;

        public static bool IsIndexError(this Exception e)
        {
            return IsOutOfMemory(e) == false &&
                   e is OperationCanceledException == false &&
                   e is IndexAnalyzerException == false &&
                   IsRavenDiskFullException(e) == false;
        }

        public static bool IsOutOfMemory(this Exception e)
        {
            return e is OutOfMemoryException || e is EarlyOutOfMemoryException || e.IsPageFileTooSmall();
        }

        public static bool IsOutOfDiskSpaceException(this Exception ioe)
        {
            var expectedDiskFullError_Posix = (int)Errno.ENOSPC;
            var expectedDiskFullError1_Win = (int)(Win32NativeFileErrors.ERROR_DISK_FULL);
            var expectedDiskFullError2_Win = (int)(Win32NativeFileErrors.ERROR_HANDLE_DISK_FULL);
            var errorCode = PlatformDetails.RunningOnPosix ? ioe.HResult : ioe.HResult & 0xFFFF;
            return PlatformDetails.RunningOnPosix ? errorCode == expectedDiskFullError_Posix : 
                                        errorCode == expectedDiskFullError1_Win || errorCode == expectedDiskFullError2_Win;
        }

        public static bool IsMediaWriteProtected(this IOException ioe)
        {
            return ioe?.Message.Contains("The media is write protected") ?? false;
        }

        public static bool IsRavenDiskFullException(this Exception e)
        {
            return e is DiskFullException;
        }

        public static bool IsPageFileTooSmall(this Exception e)
        {
            return e is Win32Exception win32Exception && win32Exception.NativeErrorCode == ERROR_COMMITMENT_LIMIT;
        }

        public static void ThrowMediaIsWriteProtected(Exception inner)
        {
            throw new IOException($"{inner.Message}. {Sparrow.Server.Platform.PalHelper.ErrorMediaIsWriteProtectedHintMessage}", inner);
                
        }
        
        public static void ThrowDiskFullException(string path) // Can be the folder path of the fole absolute path
        {
            var folderPath = Path.GetDirectoryName(path); // file Absolute Path
            var driveInfo = DiskUtils.GetDiskSpaceInfo(folderPath);
            var freeSpace = driveInfo != null ? driveInfo.TotalFreeSpace.ToString() : "N/A";
            var totalSize = driveInfo != null ? driveInfo.TotalSize.ToString() : "N/A";

            throw new DiskFullException($"There isn't enough space to flush the buffer in: {folderPath}. " +
                                        $"Currently available space: {freeSpace}/{totalSize}");
        }
    }
}
