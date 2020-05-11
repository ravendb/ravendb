using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsIndexError(this Exception e)
        {
            return IsOutOfMemory(e) == false &&
                   e is OperationCanceledException == false &&
                   IsRavenDiskFullException(e) == false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsOutOfMemory(this Exception e)
        {
            return e is OutOfMemoryException || e is EarlyOutOfMemoryException || e is Win32Exception win32Exception && IsOutOfMemory((Exception)win32Exception);
        }
        public static bool IsOutOfDiskSpaceException(this Exception ioe)
        {
            var expectedDiskFullError = PlatformDetails.RunningOnPosix ? (int)Errno.ENOSPC : (int)Win32NativeFileErrors.ERROR_DISK_FULL;
            var errorCode = PlatformDetails.RunningOnPosix ? ioe.HResult : ioe.HResult & 0xFFFF;
            return errorCode == expectedDiskFullError;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsRavenDiskFullException(this Exception e)
        {
            return e is DiskFullException;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsOutOfMemory(this Win32Exception e)
        {
            return e.NativeErrorCode == ERROR_COMMITMENT_LIMIT;
        }
    }
}
