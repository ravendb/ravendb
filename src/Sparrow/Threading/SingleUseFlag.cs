using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Threading
{
    /// <summary>
    /// A thread-safe, single use flag that can be raised once; meant to be
    /// single-user. DO NOT PASS THIS AROUND.
    /// </summary>
    /// 
    /// Example use case is a class which runs on multiple threads wants to 
    /// know if it has been disposed or not, or whether a particular event is
    /// currently happening.
    /// 
    /// For convincing on why you should use this class instead of rolling your
    /// own, see http://blog.alexrp.com/2014/03/30/dot-net-atomics-and-memory-model-semantics/
    /// and http://issues.hibernatingrhinos.com/issue/RavenDB-8260 .
    /// 
    /// PERF: This is a class instead of a struct simply because we can not
    /// verify that it won't be copied, and we don't trust our users not to
    /// copy it. It is kept so that we can move all usages back at once into
    /// structs should this be a perf issue in the future.
    public class SingleUseFlag
    {
        private int _state;

        public SingleUseFlag(SingleUseFlag other)
        {
            throw new InvalidOperationException($"Copy of {nameof(SingleUseFlag)} is forbidden");
        }

        /// <summary>
        /// Creates a flag.
        /// </summary>
        /// <param name="raised">if it should be raised or not</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SingleUseFlag()
        {
            _state = 0;
        }

        /// <summary>
        /// Creates a flag.
        /// </summary>
        /// <param name="raised">if it should be raised or not</param>
        public SingleUseFlag(bool raised = false)
        {
            _state = 0;
            if (raised)
                Interlocked.Exchange(ref _state, 1);
        }

        /// <summary>
        /// Raises the flag. If already up, throws InvalidOperationException.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RaiseOrDie()
        {
            if (Raise() == false)
                ThrowException();
        }

        /// <summary>
        /// This is here to allow RaiseOrDie() to be inlined.
        /// </summary>
        private static void ThrowException()
        {
            throw new InvalidOperationException($"Repeated Raise for a {nameof(SingleUseFlag)} instance");
        }

        /// <summary>
        /// Raises the flag
        /// </summary>
        /// <returns>If already raised, false; otherwise, true</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Raise()
        {
            return Interlocked.CompareExchange(ref _state, 1, 0) == 0;
        }

        /// <returns>True iff the flag is raised</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsRaised()
        {
            return _state != 0;
        }

        /// <summary>
        /// Returns true iff the flag is raised. Same as calling IsRaised().
        /// </summary>
        /// <param name="flag">Flag to check</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator bool(SingleUseFlag flag)
        {
            if (flag == null)
                return false;

            return flag.IsRaised();
        }
    }
}
