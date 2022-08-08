using System;
using System.ComponentModel;
using Raven.Server.Exceptions;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Platform.Posix;
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

        public static bool IsRavenDiskFullException(this Exception e)
        {
            return e is DiskFullException;
        }

        public static bool IsPageFileTooSmall(this Exception e)
        {
            return e is Win32Exception win32Exception && win32Exception.NativeErrorCode == ERROR_COMMITMENT_LIMIT;
        }
    }
}
