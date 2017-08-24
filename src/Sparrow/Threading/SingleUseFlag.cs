using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Threading
{
    /// <summary>
    /// A SingleUseNotification is sent exactly once per instance. It is thread-safe.
    /// 
    /// For convincing on why you should use this class instead of rolling your own, see
    /// http://blog.alexrp.com/2014/03/30/dot-net-atomics-and-memory-model-semantics/
    /// and http://issues.hibernatingrhinos.com/issue/RavenDB-8260 .
    /// </summary>
    public struct SingleUseFlag
    {
        private int _state;

        public SingleUseFlag(bool raised = false)
        {
            _state = 0;
            if (raised)
                Interlocked.Exchange(ref _state, 1);
        }

        /// <summary>
        /// Attempts to raise the flag. If it has already been raised, throws a InvalidOperationException.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Raise()
        {
            if (RaiseOrExit())
                ThrowException();
        }

        /// <summary>
        /// This is here to allow Raise() to be inlined.
        /// </summary>
        private static void ThrowException()
        {
            throw new InvalidOperationException($"Repeated operation for a {nameof(SingleUseFlag)} instance");
        }

        /// <summary>
        /// Attempts to raise the flag
        /// </summary>
        /// <returns>If it was already raised, returns true; otherwise, returns false</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool RaiseOrExit()
        {
            return Interlocked.CompareExchange(ref _state, 1, 0) != 0;
        }

        /// <returns>true if the flag has already been raised</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsRaised()
        {
            return _state != 0;
        }
    }
}
