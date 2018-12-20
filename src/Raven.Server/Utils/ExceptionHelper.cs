using System;
using System.Runtime.CompilerServices;
using Sparrow.LowMemory;
using Voron.Exceptions;

namespace Raven.Server.Utils
{
    public static class ExceptionHelper
    {
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
            return e is OutOfMemoryException || e is EarlyOutOfMemoryException;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsDiskFullException(this Exception e)
        {
            return e is DiskFullException;
        }

    }
}
