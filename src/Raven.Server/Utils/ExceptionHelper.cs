using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using Sparrow.LowMemory;
using Sparrow.Server.Exceptions;

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
                   IsDiskFullException(e) == false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsOutOfMemory(this Exception e)
        {
            return e is OutOfMemoryException || e is EarlyOutOfMemoryException || e is Win32Exception win32Exception && IsOutOfMemory((Exception)win32Exception);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsDiskFullException(this Exception e)
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
