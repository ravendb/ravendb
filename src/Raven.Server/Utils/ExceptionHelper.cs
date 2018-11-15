using System;
using System.Runtime.CompilerServices;
using Sparrow.LowMemory;

namespace Raven.Server.Utils
{
    public static class ExceptionHelper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsOutOfMemory(this Exception e)
        {
            return e is OutOfMemoryException || e is EarlyOutOfMemoryException;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsOperationCanceled(this Exception e)
        {
            return e is OperationCanceledException;
        }
    }
}
