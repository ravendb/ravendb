using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Threading
{
    /// <summary>
    /// A thread-safe, multiple use flag that can be raised and lowered at will; meant to be
    /// single-user.
    /// 
    /// Example use case is one class has multiple threads that access it and change the
    /// behavior externally, such as "stop logging".
    /// 
    /// For convincing on why you should use this class instead of rolling your own, see
    /// http://blog.alexrp.com/2014/03/30/dot-net-atomics-and-memory-model-semantics/ and
    /// http://issues.hibernatingrhinos.com/issue/RavenDB-8260 .
    /// 
    /// PERF: This is a struct instead of a class so that its usage may be made invisible. Do
    /// NOT change this without good reason, could have sizeable impact.
    /// </summary>
    public struct MultipleUseFlag
    {
        private int _state;

        /// <summary>
        /// Creates a flag.
        /// </summary>
        /// <param name="raised">if it should be raised or not</param>
        public MultipleUseFlag(bool raised = false)
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
            if (!Raise())
                ThrowException();
        }

        /// <summary>
        /// Lowers the flag. If already low, throws InvalidOperationException.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LowerOrDie()
        {
            if (!Lower())
                ThrowException();
        }

        /// <summary>
        /// This is here to allow RaiseOrDie() and LowerOrDie() to be inlined.
        /// </summary>
        private static void ThrowException()
        {
            throw new InvalidOperationException($"Repeated operation for a {nameof(MultipleUseFlag)} instance");
        }

        /// <summary>
        /// Lowers the flag
        /// </summary>
        /// <returns>If already low, false; otherwise, true</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Lower()
        {
            return Interlocked.CompareExchange(ref _state, 0, 1) == 1;
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
    }
}
